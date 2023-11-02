import random
import traceback
import time

from queue_date_pages import main as queue_date_pages
from queue_news import main as queue_news

from utils import utils, timeout
from db import psql


def single_run():
    """단일 실행, 예외처리 포함"""
    flag = "Start"
    start = time.time()

    # 실행
    try:
        # 1) config 가져오기
        print("> Start queue scraper")
        flag = "get config"
        date_page_ratio = utils.get_config("queue_date_page_run_n", set_int=True) / 100  # 0.0 ~ 1.0

        # 2) date_pages 실행
        print("> Start queue date_pages")
        flag = "queue.date_pages"
        random_value = random.random()  # 0~1사이의 무작위 값

        # if random_value < date_page_ratio:  # 무작위 값이 date_page_ratio 안쪽으로 나오면 실행하도록 (즉 ratio 만큼 실행되도록 (수렴하게))
        if True:
            queue_date_pages.main()
        
        # 3) queue_news 단일 실행 (date_pages에서 하나만 무작위로 가져와서 실행하는 것)
        print("> Start queue news - single date_page")
        flag = "queue.news"
        success = queue_news.main()

    # 에러 발생 시
    except Exception as e:
        error_msg = f"Error on - {flag} "
        error_traceback = traceback.format_exc()

        # TODO: 에러 로깅

        # 에러 출력
        print("-" * 60)
        print(error_msg)
        print("-" * 30)
        print(error_traceback)
        print("-" * 60)

        return False

    # 정상 종료 시
    else:
        flag = "Fin."

        # TODO: 성공 로깅

        # 결과 출력
        elapsed = time.time() - start
        print(f"> Successfully queued / elapsed: {elapsed:.2f}s")

        return True


def main():
    """반복하여 실행"""
    # TODO: 수집 시작 시 로깅

    # 반복실행
    n = 1
    while True:
        print(f"> {n}th run")
        single_run()
        time.sleep(5) # 5초 슬립하면서 실행
        n += 1
        print("#" * 40)


if __name__ == "__main__":
    main()