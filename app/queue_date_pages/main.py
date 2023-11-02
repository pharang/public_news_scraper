import os
import sys
# 현재 파일의 상위 디렉토리를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import datetime
from db import psql, model
from utils import utils


def add_date_queue(year, sid1, sid2):
    """date_pages에 특정 연도, sid1, sid2의 큐를 추가함
    sid1, sid2를 구분해서 추후 추가 sid가 추가될 때를 대응할 수 있도록

    Args:
        year(int): 연도
        sid1(int): 섹션1 아이디
        sid2(int): 섹션2 아이디
    
    Returns:
        bool: True일 경우 추가됨, False일 경우 이미 추가되어 패스함
        int: queue_count (False일 경우 이미 추가됐던 개수, True일 경우 추가한 개수)
    
    """
    # 1) queue에서 해당 year, sid1, sid2 조합이 추가된 것이 있는지 확인 (없어야 추가함)
    query = "SELECT COUNT(*) FROM queue.date_pages WHERE year = %s AND sid1 = %s AND sid2 = %s;"
    queue_count = psql.query_select(query, (year, sid1, sid2), fetchone=True)[0]

    # 이미 수집된 내용이 있는 경우
    if queue_count > 0:
        return False, queue_count

    # 2) 해당 year, sid1, sid2 셋에 대해 한국 날짜로 오늘 전까지의 큐 추가 (kst_now - 1일 까지)
    dt = datetime.datetime(year=year, month=1, day=1)
    day1_delta = datetime.timedelta(days=1)

    baseline_days = utils.get_config("date_pages_baseline_days", set_int=True)
    last_kst_dt = utils.get_kst_datetime() - datetime.timedelta(days=baseline_days)  # 추가 대상 기준 날짜 (이 날짜까지 추가)

    added_queue_count = 0
    datas = []
    while dt.year == year:  # 해당 연도 내의 날짜에서 반복
        # 기준 날짜까지만 추가하도록 (기준 날짜를 넘어갔으면 break 하도록)
        if int(last_kst_dt.strftime("%Y%m%d")) < int(dt.strftime("%Y%m%d")):
            break

        # 큐에 추가
        date = dt.strftime("%Y%m%d")
        date_queue_id = f"{sid1}-{sid2}-{date}"

        # 다음날로 dt 이동
        dt += day1_delta
        added_queue_count += 1

        # data 저장
        data =(date_queue_id, year, sid1, sid2, date, False)
        datas.append(data)

    # insert
    query = "INSERT INTO queue.date_pages (date_queue_id, year, sid1, sid2, date, page_added) VALUES (%s, %s, %s, %s, %s, %s);"
    psql.query_executemany(query, datas)
    
    # TODO: 로깅 추가
    
    return True, added_queue_count



def add_year(year):
    """현재 연도가 years, data, queue에 없을 경우 추가하는 것

    Args:
        year(int): 추가할 날짜

    Returns:
        bool: True - 성공적으로 추가됨, False 추가되지 않음
    """
    # 1) 실제로 추가되지 않았는지 체크하는 것
    years = utils.get_years(order_by_desc=True)
    if year in years:
        return False
    
    # 2) 연도 추가하기
    # info.years에 insert
    query = "INSERT INTO info.years (year) VALUES (%s);"
    psql.query_execute(query, (year,))

    # queue, data 테이블 추가
    model.create_table_queue_news(year)
    model.create_table_news(year)

    return True



def main():
    """year와 sid 조합에 대해서 date_pages 큐를 추가하기
    - 모든 years 별로 sid1, sid2 조합을 시도함, 이미 큐에 존재할 경우 추가하지 않고 없으면 추가하는 식으로
    - 이렇게 할 경우, 새롭게 year과 sid 조합을 추가할 때 자동으로 queue.date_pages도 추가되게 됨
    """
    now = utils.get_kst_datetime()

    # 1) year와 sid 조합을 모두 가져옴
    years = utils.get_years(order_by_desc=True)
    sid_sets = utils.get_sids()  # [(sid1, sid2), ...]

    # 2) 연도가 넘어갔을 경우 추가하기
    if now.year not in years:
        success = add_year(now.year)
        if success:
            print(f">> {now.year} 연도 추가 완료 (info.years, year, data, queue)")
        else:
            print(f">> {now.year} 연도 추가 실패")

    # 3) year마다 sid조합으로 중첩 for문을 돌며 date_pages 큐 추가하기
    for year in years:
        now = utils.get_kst_datetime()
        for sid1, sid2 in sid_sets:
            added, queue_count = add_date_queue(year, sid1, sid2)
            if added:
                print(f">> queue.date_pages added / Target: {year}-{sid1}-{sid2} / Added: {queue_count} / {now}")
                # TODO: 추가될 경우 로깅하도록 (추가 시점 확인하기 위함)

            # already added는 불필요한 출력으로 판단해 제외함
            # else: 
            #     print(f">> queue.date_pages already added / Target: {year}-{sid1}-{sid2} / Exists: {queue_count} / {now}")
