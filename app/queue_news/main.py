import os
import sys
# 현재 파일의 상위 디렉토리를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import time
import datetime
import json
from tqdm import tqdm

from queue_news import news_queue_scraper
from db import psql
from utils import utils


def get_target_date_page():
    """sid1, sid2, date의 조합으로 이뤄진 하나의 타겟을 랜덤으로 가져와 반환한다

    Returns:
        if 존재할 경우:
            int: date_queue_id
            int: year
            int: sid1
            int: sid2
            str: date (yyyymmdd)
        else 수집할 대상이 없을 경우:
            tuple: 비어 있는 튜플 반환 (if에서 False)
    """
    query = "SELECT date_queue_id, year, sid1, sid2, date FROM queue.date_pages WHERE page_added = %s ORDER BY RANDOM() LIMIT 1;"
    result = psql.query_select(query, (False,), fetchone=True)
    return result


def save_queue_news(year, sid1, sid2, date, links):
    """수집된 링크를 큐에 저장

    Args:
        year(int): 수집 대상 날짜의 연도(저장 테이블 구분용)
        sid1(int): 수집 대상 섹션1
        sid2(int): 수집 대상 섹션2
        date(str): 수집 대상 날짜 (yyyymmdd)
        links(list): 해당 페이지에서 수집한 뉴스 링크(전체)
    
    """
    datas = []
    kst_now = utils.get_kst_datetime()

    for link in links:
        row = (sid1, sid2, date, link, False, kst_now)
        datas.append(row)

    query = f"INSERT INTO queue.news_{year} (sid1, sid2, date, url, is_scraped, added) VALUES (%s, %s, %s, %s, %s, %s);"
    psql.query_executemany(query, datas)  # 중복검사 없이 저장



def run_queue_scraper(sid1, sid2, date, use_tqdm=False):
    """뉴스 큐 스크래퍼를 실행하고 데이터를 DB에 저장한다

    Args:
        sid1(int): 섹션 대분류 아이디
        sid2(int): 섹션 소분류 아이디
        date(str): 수집할 날짜 (yyyymmdd 형태의 문자열)
        use_tqdm(bool): tqdm 사용 여부

    Return:
        int: 마지막 페이지 (last_page)
        int: 전체 링크 수
    """
    # 1) 객체 생성
    scraper = news_queue_scraper.NewsLinksScraper(sid1, sid2, date)

    # 2) last_page 확인
    last_page = scraper.get_last_page(use_tqdm)
    
    # use_tqdm=True 시 tqdm 출력
    if use_tqdm:
        rep = tqdm(range(1, last_page+1))
    else:
        rep = range(1, last_page+1)
        
    # 3) 페이지별 수집
    links = []
    for page in rep:
        single_page_links = scraper.scrape_links_single_page(page)
        links.extend(single_page_links)
        
    # 4) 중복된 링크 업애기
    links = list(set(links))  # set 그대로 쓰는게 나을지도 (어차피 for만 돌리니까)
        
    # 5) db에 저장
    year = int(date[:4])
    save_queue_news(year, sid1, sid2, date, links)
        
    return last_page, len(links)



def update_news_id(year):
    """news_year_id와 연도를 기반으로 news_id를 업데이트

    Arg:
        year(int): 업데이트 대상 테이블의 연도
    """
    # 1) news_id가 없는 news_year_id 받아오기
    query = f"SELECT news_year_id FROM queue.news_{year} WHERE news_id IS NULL;"
    raw = psql.query_select(query)
    news_year_ids = [r[0] for r in raw]

    # 2) 업데이트용 데이터 생성
    datas = []
    for news_year_id in news_year_ids:
        str_id = str(news_year_id)

        if len(str_id) <= 8:  # 8자리까지만 사용할 예정이므로 이렇게
            str_id = str_id.zfill(8)  # 8자리로 만들고, 왼쪽을 0으로 채우기
        
        else:
            print("news_year_id가 8자리를 넘음")
            # TODO: 에러 발생하게 추가

        news_id = int(f"{year}{str_id}")  # yyyy + news_year_id[-8:] 형태의 12자리 뉴스 아이디 사용
        data = (news_id, news_year_id)
        datas.append(data)
    
    # 3) 업데이트
    query = f"UPDATE queue.news_{year} SET news_id = %s WHERE news_year_id = %s;"
    psql.query_executemany(query, datas)




def main(run_update_news_id=True):
    """스크래퍼 큐 수집 단일로 실행하고 db에 저장
    ## Note: data.news 수집 시 queue에서 미수집 뽑고 news_id가 None이 아닌 것만 가져오도록

    Args:
        update_uews_id(bool): news_id 업데이트 실행여부 | Default: True
    
    Returns:
        bool: True일 경우 수집된 것, False일 경우 모두 수집하여 수집 대상이 없는 것
    
    """
    # 1) 수집 대상 sid1, sid2, date 가져오기
    result = get_target_date_page()  # queue.date_pages에서 무작위로 가져옴
    if result:
        # 수집할 대상이 있는 경우
        date_queue_id, year, sid1, sid2, date = result
        print(f">> queue.news start - id: {date_queue_id}")
    else:
        # 수집할 대상이 없는 경우
        print(f">> queue.news - theres no not queue.date_pages target / all queue.date_pages added")
        return False

    # 2) 수집 실행하고 db에 저장하기
    max_page, links_count = run_queue_scraper(sid1, sid2, date, use_tqdm=False)

    # 3) news_id 업데이트 실행하기
    ## 뉴스 아이디가 업데이트되어야 data.news에 저장할 때 news_id가 있으므로, 기본적으로 업데이트하도록
    print(f">> queue.news - update news id")
    if run_update_news_id:
        update_news_id(year)

    # 4) queue.date_pages의 page_added, page_length, news_count 업데이트
    query = "UPDATE queue.date_pages SET page_added = %s, page_length = %s, news_count = %s WHERE date_queue_id = %s;"
    psql.query_execute(query, (True, max_page, links_count, date_queue_id))

    # 출력
    print(f">> queue.news added - {date_queue_id} / max_page: {max_page} / links_count: {links_count}")
    return True