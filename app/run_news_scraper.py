import random
import traceback
import time

from scraper_news import main as scraper_news

from utils import utils, timeout
from db import psql


# batch_size * news_timeout_multiplier 만큼을 timeout seconds 로 설정 / 윈도우에서는 사용불가
@timeout.timeout(seconds= utils.get_config("news_timeout_multiplier", True) * utils.get_config("news_batch_size", True) )
def single_run():
    """단일 실행, timeout 및 예외처리 포함"""
    flag = "Start"
    start = time.time()

    # 실행
    try:
        # 실행
        flag = "news scraper"
        success = scraper_news.main()


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

        # 에러 발생 시 10초 sleep
        time.sleep(10)

        return False

    # 정상 종료 시
    else:
        flag = "Fin."

        # TODO: 성공 로깅

        # 결과 출력
        elapsed = time.time() - start
        print(f"> Successfully scraped / elapsed: {elapsed:.2f}s")

        return True



def main():
    """timeout 핸들링하여 반복 실행"""
    # TODO: 수집 시작 시 로깅

    # 반복실행
    n = 1
    timeout_i = 1
    while True:
        try:
            print(f"> {n}th run")
            single_run()
            n += 1
            print("#" * 40)
        
        except TimeoutError:
            print("-" * 60)
            print(f"{timeout_i+1}th timeout")
            print("-" * 60)
            timeout_i += 1



if __name__ == "__main__":
    main()