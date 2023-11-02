import os
import sys
# 현재 파일의 상위 디렉토리를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import time
import datetime

import requests
from tqdm import tqdm
from bs4 import BeautifulSoup
from db import psql
from utils import utils



class NewsContentScraper():
    """뉴스 본문을 수집하는 객체
    
    Attributes:
        user_agent(str): headers에 포함할 User-Agent
        news_queue_datas(list): get_news_to_collect에서 가져와 수집하고자 하는 뉴스의 큐 리스트 (디버깅 시 필요할 수 있음)
        
    Methods:
        __init__: driver 생성하면서 initialize
        count_remains: 큐에 남은 뉴스 개수 반환
        get_queue_size: 수집할 기사가 몇개 남았는지 DB에서 검색해 오는 것
        get_news_to_collect: 스크랩되지 않은 뉴스를 n개 가져오는것
        scrape_news_content: 뉴스 하나의 정보를 수집해오는 것
        collect_news: 수집할 기사의 목록을 받아서 모두 수집을 실행하고 수집한 정보를 반환함
        save_news: collect_news에서 수집한 데이터를 db에 저장하는 것
        
    """

    
    def __init__(self):
        """webdriver, db initialize
        """
        # user_agent
        self.user_agent = utils.get_user_agent()
        
    
    # def count_remains(self):
    #     """큐에 남아있는 뉴스 개수 반환
        
    #     Returns:
    #         int, int: 수집한 개수, 전체 개수
    #     """
    #     query = "SELECT COUNT(news_id) FROM queue_news WHERE is_scraped = %s;"
    #     cnt_scraped = self.select_execute(query, (True,))[0][0]
        
    #     query = "SELECT COUNT(news_id) FROM queue_news;"
    #     cnt_all = self.select_execute(query)[0][0]
    #     return cnt_scraped, cnt_all
 
    
    # def get_news_to_collect(self, size_n=10, order_random=True):
    #     """아직 수집 되지 않은 채 queue에 있는 기사의 정보를 가져와서 반환한다.
    #     # MEMO: 여러개 가져올 수 있도록 size_n으로 대응함
    
    #     Args:
    #         size_n(int): 한번에 가져올 큐의 | default: 10
    #         order_random(bool): 가져올 때 랜덤으로 정렬할 지 여부 | default: True
        
    #     Returns:
    #         list: dict in list 형태의 기사 정보
    #             eg. [{"news_id": 뉴스 아이디, "sid1": 섹션아이디1, "sid2": 섹션아이디2, "date": 날짜, "url": url}, ...]
            
    #     """
    #     # 랜덤일 경우 랜덤 오더 사용
    #     if order_random:
    #         query = "SELECT news_id, sid1, sid2, date, url FROM queue_news WHERE is_scraped = %s ORDER BY RANDOM() LIMIT %s;"  # 랜덤 정렬
    #     else:
    #         query = "SELECT news_id, sid1, sid2, date, url FROM queue_news WHERE is_scraped = %s LIMIT %s;"
 
    #     raw = self.select_execute(query, (False, size_n))
        
    #     datas = []
    #     if raw:
    #         for news_id, sid1, sid2, date, url in raw:
    #             data = {}
    #             data["news_id"] = news_id
    #             data["sid1"] = sid1
    #             data["sid2"] = sid2
    #             data["date"] = date
    #             data["url"] = url
    #             datas.append(data)
        
    #     else:
    #         print("###################################")
    #         print("큐에 수집할 뉴스가 남아 있지 않습니다")
    #         print("###################################")
            
    #     self.news_queue_datas = datas
    #     return datas

    
    def __get_attribute_from_elements(self, elements, attribute="text", i=0, strip=True):
        """soup select 를 통해 수집한 elements 중에서 특정 element의 값을 가져온다
        # MEMO: 반복된 기능 분리함 (text, href 등의 attribute 가져오는 것)
        # MEMO: None으로 반환하는 것은, 만약 수집 중간에 페이지의 에러 등으로 인해 제대로 수집되지 않은 경우에도 멈추지 않도록 하기 위함
        # MEMO: 추후 None으로 수집된 페이지를 필터하여 제대로 수집되지 않은 내용을 체크하는 등의 배치 코드 추가
        
        Args:
            elements(list): soup 객체에서 선택하여 반환된 elements 목록
            attributes(text): 수집할 객체의 attribute | defualt: "text"
            i(int): elements에서 몇 번째 요소의 attr를 가져올 지 | default: 0
                (기본적으로 0으로 두고 사용함, 1개 요소만 가져오는 경우에 사용할 것이므로)
                i=-1일 경우 모든 값을 가져옴 (리스트 내의 모든 element에 attribute를 적용함)
            strip(bool): True일 경우 결과 텍스트값에 strip을 실행함(앞뒤 공백 제거) | default: True
                None이 아닐 경우에만 strip 실행
        
        Returns:
            string: attributes의 값, 문자 그대로 반환
            > element가 없을 경우 (==len(elements) == 0) None 반환
        """
        if elements:
            if 0 <= i:  # i가 0이상의 수(==인덱스)
                element = elements[i]

                if attribute == "text":
                    result = element.text
                else:
                    result = element[attribute]

                if strip:
                    result = result.strip()

            elif i == -1:  # i 가 -1인 경우 (==전체에서 적용)
                if attribute == "text":
                    results = [e.text for e in elements]
                else:
                    results = [e[attribute] for e in elements]

                if strip:
                    results = [r.strip() for r in results]

                result = results  # return할 때 맞춰주기 위함
                
        else:
            result = None
        
        return result

    
    def scrape_news_content(self, url):
        """뉴스 기사 하나의 html 텍스트를 받아 soup 객체를 생성하여 정보 수집 후 수집한 정보를 반환한다.
        # MEMO: lxml을 사용하여 bs4 파싱 속도 향상 기대 (pip install lxml) >> 아주 약간 향상된 듯함(2~5%쯤, 큰 차이는 없음)
        
        Args:
            url(str): 기사의 URL
        
        Returns:
            str: 기사의 URL (리다이렉트 등이 된 경우 리다이렉트 된 URL)
            dict: 수집한 정보들의 딕셔너리
                {press: 언론사(str), title: 제목(str), input: 입력시간(datetime), modify: 수정시간(datetime), 
                 writer: 기자(str), content: 본문(str), categories: 기사 내에서 분류한 카테고리의 명칭(list)}
        """
        response = requests.get(url, headers={"User-Agent": self.user_agent})
        page_url = response.url  # 리다이렉트된 페이지 url (추후 추가 필터링을 위함)
        html = response.text
        # soup = BeautifulSoup(html, "html.parser")
        soup = BeautifulSoup(html, "lxml")

        # 언론사
        selector = "#ct > div.media_end_head.go_trans > div.media_end_head_top > a > img.media_end_head_top_logo_img.light_type"
        elements = soup.select(selector)
        press = self.__get_attribute_from_elements(elements, "title", strip=True)  # attribute 값이거나 none으로 나옴 (none일 경우 해당 값이 없는것)

        # 제목
        selector = "#ct > div.media_end_head.go_trans > div.media_end_head_title"
        elements = soup.select(selector)
        title = self.__get_attribute_from_elements(elements, "text", strip=True)

        # 입력시간
        selector = "#ct > div.media_end_head.go_trans > div.media_end_head_info.nv_notrans > div.media_end_head_info_datestamp > div:nth-child(1) > span"
        elements = soup.select(selector)
        sdate = self.__get_attribute_from_elements(elements, "data-date-time", strip=True)
        if sdate:
            input_datetime = datetime.datetime.strptime(sdate, "%Y-%m-%d %H:%M:%S")
        else:
            input_datetime = None
            
        # 수정시간 (없을 수 있음)
        selector = "#ct > div.media_end_head.go_trans > div.media_end_head_info.nv_notrans > div.media_end_head_info_datestamp > div:nth-child(2) > span"
        elements = soup.select(selector)
        sdate = self.__get_attribute_from_elements(elements, "data-modify-date-time", strip=True)
        if sdate:
            modify_datetime = datetime.datetime.strptime(sdate, "%Y-%m-%d %H:%M:%S")
        else:
            modify_datetime = None
 
        # 기자 (없을 수 있음)
        selector = "#contents > div.byline > p > span"
        elements = soup.select(selector)
        writer = self.__get_attribute_from_elements(elements, "text", strip=True)

        # 본문
        selector = "#dic_area"
        elements = soup.select(selector)
        content = self.__get_attribute_from_elements(elements, "text", strip=True)
        
        # 언론사에서 분류한 카테고리
        selector = "em.media_end_categorize_item"
        elements = soup.select(selector)
        categories = self.__get_attribute_from_elements(elements, "text", strip=True, i=-1)  # 리스트로 받아옴
        
        # 딕셔너리화 및 반환
        news = {}
        news["press"] = press
        news["title"] = title
        news["input"] = input_datetime
        news["modify"] = modify_datetime
        news["writer"] = writer
        news["content"] = content
        news["categories"] = categories
        return page_url, news
    
    
    def collect_news(self, to_collect_news_info):
        """수집할 기사의 정보를 받아온 뒤, 데이터를 수집하여 리스트로 반환한다.
        
        Args:
            to_collect_news_info(list): list in list 형태의 기사 정보
                [(news_id, sid1, sid2, date, url), ...]
        
        Returns:
            list: 리스트 안의 딕셔너리 형태
                dict: 수집한 정보들의 딕셔너리 (해당 정보들 기반으로 DB에 저장)
                    {news_year_id: 연도내 뉴스아이디, news_id: 뉴스아이디(int), sid1: 섹션아이디(int), sid2: 섹션아이디(int), date: 날짜(str), 
                     page_url: 최종 페이지 링크(str), kst_now: 수집 시 시간(datetime),
                     press: 언론사(str), title: 제목(str), input: 입력시간(datetime), modify: 수정시간(datetime), 
                     writer: 기자(str), content: 본문(str), categories: 기사 내에서 분류한 카테고리의 명칭(list)}
        """
        news_datas = []
        
        for news_year_id, news_id, sid1, sid2, date, url in to_collect_news_info:
            news_data = {}
            news_data["news_year_id"] = news_year_id
            news_data["news_id"] = news_id
            news_data["sid1"] = sid1
            news_data["sid2"] = sid2
            news_data["date"] = date
            news_data["url"] = url
            
            # 페이지 수집
            page_url, news = self.scrape_news_content(url)
            news_data["page_url"] = page_url
            
            # 현재시간 (수집시간)
            news_data["kst_now"] = utils.get_kst_datetime()
           
            # 딕셔너리 하나로 합침
                # {news_id: 뉴스아이디(int), sid1: 섹션아이디(int), sid2: 섹션아이디(int), date: 날짜(str), 
                #  page_url: 최종 페이지 링크(str), kst_now: 수집 시 시간(datetime),
                #  press: 언론사(str), title: 제목(str), input: 입력시간(datetime), modify: 수정시간(datetime), 
                #  writer: 기자(str), content: 본문(str), categories: 기사 내에서 분류한 카테고리의 명칭(list)}
            for k, v in news.items():
                news_data[k] = v
            
            # 합침
            news_datas.append(news_data)

        # 반환
        return news_datas


    # def save_news(self, datas):
    #     """collect_news로 수집한 데이터를 기반으로 DB에 insert하고 업데이트하기
    #     - 테스트의 경우 해당 메소드를 실행하지 않으면 됨
    #     # MEMO: 롤백 기능 추가함
        
    #     Args:
    #         datas(list): 아래와 같은 형태의 수집한 데이터 딕셔너리가 리스트 안에 담긴 형태
    #                 [
    #                 {news_id: 뉴스아이디(int), sid1: 섹션아이디(int), sid2: 섹션아이디(int), date: 날짜(str), 
    #                  page_url: 최종 페이지 링크(str), kst_now: 수집 시 시간(datetime),
    #                  press: 언론사(str), title: 제목(str), input: 입력시간(datetime), modify: 수정시간(datetime), 
    #                  writer: 기자(str), content: 본문(str), categories: 기사 내에서 분류한 카테고리의 명칭(list)}
    #                  ...
    #                  ]
    #     """
        
    #     # 뉴스 저장용 쿼리 데이터 준비
    #     news_datas_to_db = []
    #     for news in datas:
    #         data = (
    #             news["year_news_id"], news["news_id"], news["sid1"], news["sid2"], news["date"], news["page_url"], news["kst_now"],
    #             news["press"], news["title"], news["input"], news["modify"], news["writer"], news["content"], news["categories"]
    #         )
    #         news_datas_to_db.append(data)
            
    #     # 큐 업데이트용 쿼리 데이터 준비
    #     queue_datas_to_db = []
    #     for news in datas:
    #         data = (True, news["kst_now"], news["news_id"])
    #         queue_datas_to_db.append(data)
        
    #     # 쿼리 실행 (예외처리(롤백) 적용)
    #     error_flag = True 
    #     try:
    #         # 수집한 뉴스 저장
    #         query = "INSERT INTO news (news_id, sid1, sid2, date, page_url, scraped, press, title, input_date, modify_date, writer, content, categories) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    #         self.query_executemany(query, news_datas_to_db)

    #         # 큐에서 수집한 뉴스들 is_scraped로 변경
    #         query = "UPDATE queue_news SET is_scraped = %s, scraped = %s WHERE news_id = %s;"
    #         self.query_executemany(query, queue_datas_to_db)
            
    #         # raise Exception("에러 발생")   # 테스트용
    #         error_flag = False  # 에러 없이 종료된 경우 flag 설정
            
    #     finally:
    #         # error_flag가 True일 경우에만 롤백 작업을 실시함
    #         # try-except가 아니라 try-finally로 구현하여 에러는 그대로 발생시킨 채로 롤백만 실행함
    #         if error_flag:
    #             news_ids = [news["news_id"] for news in datas]
                
    #             # 데이터 롤백(삭제)
    #             query = "DELETE FROM news WHERE news_id = %s;"
    #             query_datas = [(news_id,) for news_id in news_ids]
    #             self.query_executemany(query, query_datas)
                
    #             # 큐 롤백(False로 업데이트)
    #             query = "UPDATE queue_news SET is_scraped = %s WHERE news_id = %s;"
    #             query_datas = [(False, news_id) for news_id in news_ids]
    #             self.query_executemany(query, query_datas)
                
                
    #     # 테스트용 리턴
    #     return news_datas_to_db, queue_datas_to_db
        
                