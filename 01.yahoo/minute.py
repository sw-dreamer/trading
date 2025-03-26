import pandas as pd
import yfinance as yf
import datetime
import logging
import os
import mysql.connector
from sqlalchemy import create_engine, Column, String, Numeric, DateTime, BigInteger, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
import schedule

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("minute_stock_data.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# SQLAlchemy Base 생성
Base = declarative_base()

# 타임존 설정
US_Eastern = pytz.timezone('US/Eastern')
KST = pytz.timezone('Asia/Seoul')

class ComprehensiveMinuteStockData(Base):
    """
    SQLAlchemy ORM 모델 - 포괄적인 분단위 주식 데이터
    """
    __tablename__ = 'comprehensive_minute_stock_data'

    Ticker = Column(String(50), primary_key=True)
    Timestamp = Column(DateTime, primary_key=True)
    Open = Column(Numeric(20, 4))
    High = Column(Numeric(20, 4))
    Low = Column(Numeric(20, 4))
    Close = Column(Numeric(20, 4))
    Volume = Column(BigInteger)
    
    # 추가적인 상세 정보 필드
    Dividends = Column(Numeric(20, 4))
    Stock_Splits = Column(Float)
    Capital_Gains = Column(Numeric(20, 4))
    
    # 추가 가격 및 거래 관련 정보
    Previous_Close = Column(Numeric(20, 4))
    Market_Price = Column(Numeric(20, 4))
    Bid_Price = Column(Numeric(20, 4))
    Ask_Price = Column(Numeric(20, 4))
    Bid_Size = Column(BigInteger)
    Ask_Size = Column(BigInteger)
    
    # 가격 변동 관련 정보
    Price_Change = Column(Numeric(20, 4))
    Price_Change_Percent = Column(Numeric(20, 4))

def create_comprehensive_minute_stock_table(mysql_config, table_name):
    """
    포괄적인 분단위 주식 데이터를 저장할 MySQL 테이블 생성
    """
    try:
        # MySQL 연결
        conn = mysql.connector.connect(
            host=mysql_config['host'],
            port=mysql_config['port'],
            user=mysql_config['user'],
            password=mysql_config['password'],
            database=mysql_config['database']
        )
        cursor = conn.cursor()

        # 테이블 생성 쿼리 (포괄적인 분단위 데이터 저장)
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            `Ticker` VARCHAR(50) NOT NULL,
            `Timestamp` TIMESTAMP NOT NULL,
            `Open` DECIMAL(20,4),
            `High` DECIMAL(20,4),
            `Low` DECIMAL(20,4),
            `Close` DECIMAL(20,4),
            `Volume` BIGINT,
            `Dividends` DECIMAL(20,4),
            `Stock_Splits` FLOAT,
            `Capital_Gains` DECIMAL(20,4),
            `Previous_Close` DECIMAL(20,4),
            `Market_Price` DECIMAL(20,4),
            `Bid_Price` DECIMAL(20,4),
            `Ask_Price` DECIMAL(20,4),
            `Bid_Size` BIGINT,
            `Ask_Size` BIGINT,
            `Price_Change` DECIMAL(20,4),
            `Price_Change_Percent` DECIMAL(20,4),
            PRIMARY KEY (`Ticker`, `Timestamp`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
        
        # 테이블 생성 실행
        cursor.execute(create_table_query)
        conn.commit()
        
        logger.info(f"MySQL 테이블 '{table_name}' 생성 완료")
        
        # 연결 종료
        cursor.close()
        conn.close()
        
        return True
        
    except mysql.connector.Error as e:
        logger.error(f"MySQL 테이블 생성 중 오류 발생: {e}")
        return False

def extract_tickers(csv_path):
    """
    CSV 파일에서 ticker 추출
    """
    try:
        logger.info(f"CSV 파일 '{csv_path}'에서 데이터 읽기 시작")
        df = pd.read_csv(csv_path, encoding='utf-8-sig')

        logger.info(f"CSV 파일 읽기 완료. 총 {len(df)} 개의 항목 발견")
        
        if 'ticker' not in df.columns:
            logger.error("CSV 파일에 'ticker' 열이 없습니다")
            return []
        
        tickers = df['ticker'].tolist()
        
        logger.info(f"{len(tickers)} 개의 ticker 추출: {tickers[:5]}{'...' if len(tickers) > 5 else ''}")
        
        return tickers
    
    except Exception as e:
        logger.error(f"CSV 파일 읽기 중 오류 발생: {str(e)}")
        return []

def safe_convert_to_float(value, default=None):
    """
    안전하게 float로 변환하는 함수
    """
    try:
        if value is None or value == 'N/A' or value == '':
            return default
        if isinstance(value, str):
            value = value.replace(',', '')
        return float(value)
    except (ValueError, TypeError):
        logger.warning(f"float 변환 실패: {value}")
        return default

def get_comprehensive_minute_stock_data(ticker_symbol, max_retries=3):
    """
    단일 Ticker의 포괄적인 분단위 주식 데이터 추출
    """
    retries = 0
    while retries < max_retries:
        try:
            logger.info(f'Comprehensive minute data for ticker: {ticker_symbol} 시작')

            # 요청 간 지연
            time.sleep(random.uniform(0.5, 1.5))

            # Yahoo Finance Ticker 객체 생성
            ticker = yf.Ticker(ticker_symbol)
            
            # 1분 간격 데이터 추출 (최근 1거래일)
            hist_data = ticker.history(period='1d', interval='1m')
            
            if hist_data.empty:
                logger.warning(f"{ticker_symbol}에 대한 최신 데이터를 찾을 수 없습니다.")
                return None

            # 데이터 변환 및 수집
            comprehensive_data = []
            for timestamp, row in hist_data.iterrows():
                # 각 분봉에 대한 데이터 생성
                minute_data = {
                    'Ticker': ticker_symbol,
                    'Timestamp': timestamp.to_pydatetime(),
                    'Open': safe_convert_to_float(row['Open']),
                    'High': safe_convert_to_float(row['High']),
                    'Low': safe_convert_to_float(row['Low']),
                    'Close': safe_convert_to_float(row['Close']),
                    'Volume': safe_convert_to_float(row['Volume'], 0),
                    'Dividends': safe_convert_to_float(row['Dividends']),
                    'Stock_Splits': safe_convert_to_float(row['Stock Splits']),
                    
                    # 추가 정보 계산
                    'Previous_Close': safe_convert_to_float(hist_data['Close'].shift(1).iloc[-1]),
                    'Market_Price': safe_convert_to_float(row['Close']),
                    'Price_Change': safe_convert_to_float(row['Close'] - row['Open']),
                    'Price_Change_Percent': safe_convert_to_float((row['Close'] - row['Open']) / row['Open'] * 100)
                }
                
                # 호가 정보 추가 (실제 데이터가 없는 경우 근사치 사용)
                minute_data['Bid_Price'] = safe_convert_to_float(row['Low'])
                minute_data['Ask_Price'] = safe_convert_to_float(row['High'])
                minute_data['Bid_Size'] = safe_convert_to_float(row['Volume'] // 2, 0)
                minute_data['Ask_Size'] = safe_convert_to_float(row['Volume'] // 2, 0)
                
                # 추가적인 자본 이득 계산 (근사치)
                minute_data['Capital_Gains'] = safe_convert_to_float(row['Close'] - row['Open'])
                
                comprehensive_data.append(minute_data)

            return comprehensive_data

        except Exception as e:
            if "Too Many Requests" in str(e) or "Rate limit" in str(e):
                # 요청 제한 시 지수적 백오프
                retries += 1
                backoff_time = 2 ** retries
                logger.warning(f"Rate limit exceeded for {ticker_symbol}. Retrying in {backoff_time} seconds.")
                time.sleep(backoff_time)
                continue
            else:
                logger.error(f"Error extracting comprehensive minute data for {ticker_symbol}: {e}")
                return None

    logger.error(f"Max retries exceeded for {ticker_symbol}")
    return None

def get_all_comprehensive_minute_stock_data(tickers, max_workers=5):
    """
    병렬 처리를 활용한 포괄적인 분단위 주식 데이터 추출
    """
    comprehensive_data = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker = {executor.submit(get_comprehensive_minute_stock_data, ticker): ticker for ticker in tickers}
        
        for future in as_completed(future_to_ticker):
            result = future.result()
            if result:
                comprehensive_data.extend(result)
    
    return pd.DataFrame(comprehensive_data)

def save_comprehensive_minute_stock_data_to_mysql(dataframe, table_name, mysql_config):
    """
    MySQL에 포괄적인 분단위 데이터 저장 시 배치 처리 및 성능 최적화
    """
    try:
        # 테이블 생성 함수 호출
        create_comprehensive_minute_stock_table(mysql_config, table_name)
        
        # SQLAlchemy 엔진 생성
        connection_string = f"mysql+mysqlconnector://{mysql_config['user']}:{mysql_config['password']}@{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"
        engine = create_engine(connection_string, pool_size=10, max_overflow=20)
        
        # 배치 사이즈 설정
        batch_size = 100
        total_rows = len(dataframe)
        
        # 각 배치별로 처리
        for i in range(0, total_rows, batch_size):
            batch = dataframe.iloc[i:i+batch_size]
            batch.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            logger.info(f"{i+batch_size}/{total_rows} 개의 포괄적인 분단위 데이터 MySQL에 저장 완료")
    
    except SQLAlchemyError as e:
        logger.error(f"MySQL에 포괄적인 분단위 데이터 저장 중 오류 발생: {e}")

def periodic_comprehensive_minute_stock_data_update(csv_path, mysql_config, table_name, interval_minutes=1):
    """
    주기적으로 포괄적인 분단위 주식 데이터를 업데이트하는 함수
    """
    logger.info(f'='*1000)
    logger.info(f'{interval_minutes}분 주기 포괄적인 분단위 주식 데이터 업데이트 시작')

    # 티커 추출
    tickers = extract_tickers(csv_path)
    
    if not tickers:
        logger.error("티커를 찾을 수 없습니다. 업데이트를 건너뜁니다.")
        return

    # 포괄적인 분단위 주식 데이터 추출
    comprehensive_stock_df = get_all_comprehensive_minute_stock_data(tickers)

    if comprehensive_stock_df.empty:
        logger.error("추출된 포괄적인 분단위 주식 데이터가 없습니다. 업데이트를 건너뜁니다.")
        return

    # MySQL에 저장
    save_comprehensive_minute_stock_data_to_mysql(comprehensive_stock_df, table_name, mysql_config)
    
    logger.info(f"포괄적인 분단위 업데이트 완료: {len(comprehensive_stock_df)} 개의 데이터 포인트 추출")

def main():
    # 설정 변수
    csv_path = 'C:\\Users\\TJ\\Desktop\\tra\\tech_sector_tickers.csv'  # CSV 파일 경로
    mysql_config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'rootpassword',
        'database': 'stock'
    }
    table_name = 'comprehensive_minute_stock_data'
    
    # 초기 1회 실행
    periodic_comprehensive_minute_stock_data_update(csv_path, mysql_config, table_name)

    # 1분마다 주기적으로 업데이트
    schedule.every(1).minutes.do(
        periodic_comprehensive_minute_stock_data_update, 
        csv_path=csv_path, 
        mysql_config=mysql_config, 
        table_name=table_name
    )

    # 스케줄러 실행
    logger.info("포괄적인 분단위 주기적 업데이트 스케줄러 시작")
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
