import os
import sys
# 현재 파일의 상위 디렉토리(app)를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import psql


def initialize_info_years(first_year=2005, last_year=2023):
    query = "INSERT INTO info.years (year, dates_queued) VALUES (%s, %s);"
    for year in range(first_year, last_year+1):
        psql.query_execute(query, (year, False))


def initialize_info_sids():
    """sid1, sid2 초기화
    - 우선 경제 섹션만 추가하여 사용함
    sid1: 101_경제, 100_정치, 102_사회, 103_생활/문화, 105_IT/과학, 104_세계
    """
    # 경제 섹션 추가
    sid1 = [101, "경제"]
    sid2_all = [
        [256, "금융"],
        [258, "증권"], 
        [261, "산업/재계"], 
        [771, "중기/벤쳐"], 
        [260, "부동산"], 
        [262, "글로벌 경제"], 
        [310, "생활경제"], 
        [263, "경제일반"], 
    ]
    for s2_code, s2_name in sid2_all:
        s1_code, s1_name = sid1
        psql.query_execute("INSERT INTO info.sids (sid1, sid1_name, sid2, sid2_name) VALUES (%s, %s, %s, %s);", 
                           (s1_code, s1_name, s2_code, s2_name))
                        
    # TODO: 정치 섹션 추가
    # TODO: 사회 섹션 추가
    # TODO: 생활/문화 섹션 추가
    # TODO: IT/과학 섹션 추가
    # TODO: 세계 섹션 추가


def initialize_info_config():
    """config 초기화
    user_agent, news_batch_size 를 기본으로 추가
    """
    key_value = [
        ("user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"),  # user_agent
        ("news_batch_size", "100"),  # news_batch_size
        ("date_pages_baseline_days", "1"),  # kst_now 기준 며칠 전까지 큐에 추가할지
        ("queue_date_page_run_n", "20"),  # queue 100번 실행 마다 date_page를 실행할 횟수 (0 ~ 100)
        ("news_timeout_multiplier", "2")  # data.news 수집 시 batch_size * n으로 timeout 초 설정할 때 사용할 n
    ]

    # config 추가
    for k, v in key_value:
        query = "INSERT INTO info.config (key, value) VALUES (%s, %s);"
        psql.query_execute(query, (k, v))


def main():
    """info.years, info.sids, info.config 테이블에 기본 정보 추가하기
    """
    print("initialize - info.years")
    initialize_info_years()
    print("initialize - info.sids")
    initialize_info_sids()
    print("initialize - info.config")
    initialize_info_config()
    print("finished")



if __name__ == "__main__":
    main()