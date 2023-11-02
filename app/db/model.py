import os
import sys
# 현재 파일의 상위 디렉토리(app)를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from db import psql


def create_schema():
    """스키마 생성"""
    # info 스키마 생성
    query = "CREATE schema IF NOT EXISTS info;"
    psql.query_execute(query)

    # queue 스키마 생성
    query = "CREATE schema IF NOT EXISTS queue;"
    psql.query_execute(query)

    # data 스키마 생성
    query = "CREATE schema IF NOT EXISTS data;"
    psql.query_execute(query)



def create_tables_for_info():
    """info 스키마의 테이블 생성"""
    # info.years 생성
    query = """
    CREATE TABLE info.years (
        year INT PRIMARY KEY,
        dates_queued BOOLEAN
    );"""
    psql.query_execute(query)

    # info.sids 생성
    query = """
    CREATE TABLE info.sids (
        sid1 INT NOT NULL,
        sid1_name TEXT NOT NULL,
        sid2 INT PRIMARY KEY,
        sid2_name TEXT NOT NULL
    );"""
    psql.query_execute(query)

    # info.config 생성
    query = """
    CREATE TABLE info.config (
        key TEXT PRIMARY KEY,
        value TEXT
    );"""
    psql.query_execute(query)



def create_table_queue_news(year):
    """연도를 받아 해당 연도의 큐 테이블을 생성"""
    # queue.news_{연도} 생성
    query = f"""
    CREATE TABLE queue.news_{year} (
        news_year_id SERIAL PRIMARY KEY,
        news_id BIGINT UNIQUE,
        sid1 INT NOT NULL,
        sid2 INT NOT NULL,
        date TEXT,
        url TEXT NOT NULL,
        is_scraped BOOLEAN NOT NULL,
        added TIMESTAMP,
        scraped TIMESTAMP
    );"""
    psql.query_execute(query)



def create_table_queue_date_page():
    """date_pages 테이블 생성"""
    # queue.date_page 생성
    query = """
    CREATE TABLE queue.date_pages (
        date_queue_id TEXT PRIMARY KEY,
        year INT NOT NULL,
        date TEXT NOT NULL,
        sid1 INT NOT NULL,
        sid2 INT NOT NULL,
        page_added BOOLEAN NOT NULL,
        page_length INT,
        news_count INT
    );"""
    psql.query_execute(query)



def create_table_news(year):
    """연도를 받아 해당 연도의 뉴스 테이블을 생성"""
    # data.news_{연도} 생성
    query = f"""
    CREATE TABLE data.news_{year} (
        news_year_id INT PRIMARY KEY,
        news_id BIGINT UNIQUE,
        sid1 INT,
        sid2 INT,
        date TEXT,
        page_url TEXT,
        scraped TIMESTAMP,
        last_updated TIMESTAMP,
        title TEXT,
        press TEXT,
        input_date TIMESTAMP,
        modify_date TIMESTAMP,
        writer TEXT,
        content TEXT,
        categories TEXT[]
    );"""
    psql.query_execute(query)

    # data.news_addition_{연도} 생성
    query = f"""
    CREATE TABLE data.news_addition_{year} (
        news_year_id INT PRIMARY KEY,
        news_id BIGINT UNIQUE,
        keys TEXT[],
        values JSONB
    );"""
    psql.query_execute(query)



def create_temp_log_table():
    """postgresql db에 임시로 사용하는 로그 테이블을 생성"""
    query = f"""
    CREATE TABLE logs (
        _id SERIAL PRIMARY KEY,
        log JSONB
    );"""
    psql.query_execute(query)

# if __name__ == "__main__":
#     create_temp_log_table()