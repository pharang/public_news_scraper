import os
import sys
# 현재 파일의 상위 디렉토리를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import time
import datetime
import json
from tqdm import tqdm

from scraper_news import news_content_scraper
from db import psql
from utils import utils



def get_not_collected_count(year):
    """해당 연도에 수집되지 않은 뉴스의 개수를 반환한다.

    Args:
        year(int): 대상 큐의 연도
    
    Returns:
        int: 수집되지 않은 뉴스의 개수
    """
    query = f"SELECT COUNT(*) FROM queue.news_{year} WHERE is_scraped = %s;"
    raw = psql.query_select(query, (False,), fetchone=True)
    not_collected_count = raw[0]
    return not_collected_count



def get_news_to_collect(year, size_n=100):
    """아직 수집 되지 않은 채 queue에 있는 기사의 정보를 가져와서 반환한다.
    # MEMO: 여러개 가져올 수 있도록 size_n으로 대응함, 랜덤으로 가져옴


    Args:
        year(int): 큐를 가져올 
        size_n(int): 한번에 가져올 큐의 | default: 100
    
    Returns:
        list: list(tuple) in list 형태의 기사 정보
            [(news_year_id, news_id, sid1, sid2, date, url), ...]
            큐에 내용이 없을 경우 빈 리스트 반환 (if문으로 처리 예정)
        
    """
    # is_scraped = False & news_id가 null이 아닌 큐 중 랜덤으로 정렬 후 n개 가져오는
    query = f"""
    SELECT news_year_id, news_id, sid1, sid2, date, url FROM queue.news_{year}
        WHERE is_scraped = %s AND news_id IS NOT NULL
        ORDER BY RANDOM() LIMIT %s;"""
    queue_info = psql.query_select(query, (False, size_n))

    if queue_info:
        # 큐가 존재할 경우
        return queue_info  # [(news_year_id, news_id, sid1, sid2, date, url), ...]
    
    else:
        # 큐에 수집할 내용이 없을 경우
        return []  # 빈 리스트 반환



def save_news(year, news_datas):
    """수집한 뉴스 데이터를 저장한다

    Args:
        year(int): 수집한 대상 연도 (저장 대상 연도)
        news_datas(list): 아래와 같은 형태의 dict in list
            [
               {news_year_id: 연도내 뉴스아이디, news_id: 뉴스아이디(int), sid1: 섹션아이디(int), sid2: 섹션아이디(int), date: 날짜(str), 
                page_url: 최종 페이지 링크(str), kst_now: 수집 시 시간(datetime),
                press: 언론사(str), title: 제목(str), input: 입력시간(datetime), modify: 수정시간(datetime), 
                writer: 기자(str), content: 본문(str), categories: 기사 내에서 분류한 카테고리의 명칭(list)}
            ...
            ]
        
    Returns:
        list: news_year_id 전체 리스트
        list: 저장 대상 news_year_id 리스트

    """
    # 1) 이미 수집된 news_year_id는 제외하기
    news_year_ids = [news_data['news_year_id'] for news_data in news_datas]
    query = f"SELECT news_year_id, is_scraped FROM queue.news_{year} WHERE news_year_id IN %s;"
    raw = psql.query_select(query, (tuple(news_year_ids),))
    collected_news_year_ids = [news_year_id for news_year_id, is_scraped in raw if is_scraped]
    target_news_year_ids = [news_year_id for news_year_id, is_scraped in raw if not is_scraped]

    # 2) 쿼리용 데이터 준비
    # 뉴스 저장용 쿼리 데이터 준비
    news_datas_to_db = []
    queue_datas_to_db = []
    for news in news_datas:
        if news['news_year_id'] not in collected_news_year_ids:
            # 뉴스 저장용 쿼리 데이터
            data = (
                news["news_year_id"], news["news_id"], news["sid1"], news["sid2"], news["date"], news["page_url"], news["kst_now"],
                news["press"], news["title"], news["input"], news["modify"], news["writer"], news["content"], news["categories"]
            )
            news_datas_to_db.append(data)

            # 큐 업데이트용 쿼리 데이터
            data = (True, news["kst_now"], news["news_year_id"])
            queue_datas_to_db.append(data)


    # 3) 예외처리(롤백 사용하여 데이터 insert, update)
    # 쿼리 실행 (예외처리(롤백) 적용)
    error_flag = True 
    try:
        # 수집한 뉴스 저장
        query = f"""INSERT INTO data.news_{year} (news_year_id, news_id, sid1, sid2, date, page_url, scraped, press, title, input_date, modify_date, writer, content, categories) 
                                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        psql.query_executemany(query, news_datas_to_db)

        # 큐에서 수집한 뉴스들 is_scraped로 변경
        query = f"UPDATE queue.news_{year} SET is_scraped = %s, scraped = %s WHERE news_year_id = %s;"
        psql.query_executemany(query, queue_datas_to_db)

        # TODO: 수집 완료 로깅하기

        
        # 에러 없이 종료된 경우 flag 설정
        error_flag = False  
        
    finally:
        # error_flag가 True일 경우에만 롤백 작업을 실시함
        # try-except가 아니라 try-finally로 구현하여 에러는 그대로 발생시킨 채로 롤백만 실행함 > 에러는 run에서 한번에 처리
        if error_flag:
            # 데이터 롤백(삭제)
            query = f"DELETE FROM data.news_{year} WHERE news_year_id = %s;"
            query_datas = [(news_year_id,) for news_year_id in target_news_year_ids]
            psql.query_executemany(query, query_datas)
            
            # 큐 롤백(False로 업데이트)
            query = f"UPDATE queue.news_{year} SET is_scraped = %s WHERE news_year_id = %s;"
            query_datas = [(False, news_year_id,) for news_year_id in target_news_year_ids]
            psql.query_executemany(query, query_datas)

            # TODO: 롤백 시 로깅하기
    
    return news_year_ids, target_news_year_ids




def main():
    """뉴스 데이터 수집 실행

    Returns:
        bool: True일 경우 제대로 수집 후 저장된 것 / False일 경우 모두 수집된 것
    """
    # 1) batch_size, years 준비
    batch_size = utils.get_config("news_batch_size", set_int=True)
    years = utils.get_years()
    print(f">> Start scraping news / batch: {batch_size}")

    # 2) year 내림차순으로 내려오며 수집 대상 큐 남아있는 연도 체크
    target_year = False
    for year in years:
        to_collect_count = get_not_collected_count(year)
        print(f">> {year}'s not collected count = {to_collect_count}")
        if to_collect_count > 0:
            target_year = year  # 수집 대상 연도 설정 (count가 1이상 남아있는 경우)
            break
    print(f">> Target year: {target_year}")
    
    # 모든 연도의 큐에서 미수집 뉴스가 없을 경우 > 종료
    if not target_year:
        print(">> All queue scraped")
        return False

    # 3) 큐에서 정보 가져오기
    queue_info = get_news_to_collect(target_year, batch_size)

    # 4) 뉴스 데이터 수집
    news = news_content_scraper.NewsContentScraper()
    news_datas = news.collect_news(queue_info)
    print(">> Scraped")

    # 5) DB에 저장 & 로깅
    save_news(target_year, news_datas)
    print(">> Saved")

    return True

