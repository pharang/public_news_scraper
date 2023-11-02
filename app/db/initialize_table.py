import os
import sys
# 현재 파일의 상위 디렉토리(app)를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import model
from utils import utils



def main(first_year=2005, last_year=2023):
    """테이블을 초기화한다

    Args:
        first_year(int): 큐, 뉴스 테이블의 첫 연도
        last_year(int): 큐, 뉴스 테이블의 마지막 연도
    """
    # 스키마 생성
    model.create_schema()
    print("스키마 생성")

    # info
    model.create_tables_for_info()
    print("info 스키마의 테이블 생성")

    # queue.date_page
    model.create_table_queue_date_page()
    print("queue.date_page 테이블 생성")

    # 연도별 큐, 뉴스 테이블
    for year in range(first_year, last_year+1):
        model.create_table_queue_news(year)
        model.create_table_news(year)
        print(f"queue.news_{year}, data.news_{year}, data.news_addition_{year} 테이블 생성")



if __name__ == "__main__":

    print("#" * 50)
    answer = input("테이블을 생성할까요?(y/n): ")

    if answer in ["y", "Y"]:
        main(last_year=utils.get_kst_datetime().year)
    
    print("테이블 생성 완료")
    print("#" * 50)

