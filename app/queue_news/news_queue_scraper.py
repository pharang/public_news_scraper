import os
import sys
# 현재 파일의 상위 디렉토리를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import time

import requests
from tqdm import tqdm
from bs4 import BeautifulSoup
from db import psql
from utils import utils


class NewsLinksScraper():
    """특정 소분류(sid1 & sid2) 섹션의 뉴스 기사 링크 목록을 수집하기 위한 객체
    sid1, sid2와 일자를 받아 해당 일자의 뉴스 기사 링크를 수집한다.
    ## MEMO: run을 제외한 다른 메소드들에서는 객체 변수 업데이트를 가능한 하지 않는 방향으로 작성하기 (직관적 구조)
 
    Attributes:
        sid1(int): 섹션 대분류 아이디
        sid2(int): 섹션 소분류 아이디
        date(str): 수집할 날짜
        
        user_agent(str): headers에 포함할 User-Agent, 기본값이 존재하고 필요에 따라 객체 초기화 후 덮어써서 사용함
        start_time(float): 객체가 초기화된 시간 (=시작 시간)
        elapsed(float): 소요 시간, start_time - time.time()
        
    
    Methods:
        __init__: sid와 날짜를 받고 initialize
        get_link_page_soup: 특정 링크 페이지에 req > html > soup 생성하여 반환함
        get_last_page: 현재 설정의 링크 페이지의 마지막 숫자를 반환
        scrape_single_link_page: 링크 페이지 하나에서 링크를 모두 수집하여 반환
        run: 위 과정을 차레로 실행하여 특정 날짜의 모든 링크 수집 // 기본적으론 initialize후 run만 실행하는 식으로
        
    """
    
    def __init__(self, sid1=101, sid2=259, date="20220101"):
        """변수를 초기화하고, last_page를 수집하여 설정한다.
        
        Args:
            sid1(int): 섹션 대분류 아이디
            sid2(int): 섹션 소분류 아이디
            date(str): 수집할 날짜 (yyyymmdd 형태의 문자열)
        """
        # initialize
        self.sid1 = sid1
        self.sid2 = sid2
        self.date = date
        
        self.user_agent = utils.get_user_agent()
        self.start_time = time.time()
        self.elapsd = 0
        
    
    
    def get_link_page_soup(self, page=1):
        """특정 페이지의 링크 페이지에 접속한 뒤 soup 객체를 생성하여 반환한다.
        타 메소드에서 반복적으로 사용되는 기능을 뺀 것
        - sid1, sid2, date, user_agent는 객체 변수를 사용하고, page만 받음
        - page를 여기에선 업데이트하지 않음 (가능한 가장 간단한 기능으로 구현하기)
        
        Args:
            page(int): 접속할 페이지의 숫자
        
        Returns:
            BeautifulSoup: bs4 객체
        """
        args = {"mode": "LS2D", "mid": "shm",  # mid는 main content 화면의 구성에 관련, mode는 미확인
                "sid1": self.sid1, "sid2": self.sid2,
                "date": self.date, "page": page}
        response = requests.get("https://news.naver.com/main/list.naver", params=args,
                            headers={"User-Agent": self.user_agent})
        soup = BeautifulSoup(response.text, "html.parser")
        return soup

    
    def get_last_page(self, use_tqdm=False):
        """현재 세팅(sid1, sid2, date)하에서 뉴스 기사 목록의 마지막 페이지를 반환한다.
        
        Args:
            use_tqdm(bool): tqdm을 통한 출력을 실행할 지 여부 | default: False
            
        Returns:
            int: 마지막 페이지 숫자
        
        MEMO(0612): last_page를 못 불러오는 문제가 생겨서(20210129, 101, 262) if 페이가 10페이지 이하일 경우 별도로 처리하게 수정
        """
        # 페이지 여러 페이지인지 체크
        soup = self.get_link_page_soup(1)
        pages = soup.select("#main_content > div.paging > a")  # 페이지 목록들 가져옴
        
        # Case: 1페이지만 있을 경우 (현재 페이지는 카운트되지 않으므로, 0으로 길이가 잡힘)
        if len(pages) == 0:
            last_page = 1
                       
        # Case: 페이지가 10장 이하 > last_page를 현재 페에지에서 맨 마지막으로
        elif len(pages) < 10:  # 현재 페이지(1p)를 제외하므로, 10개 미만(==총 페이지 11페이지 이하)
            last_page = int(pages[-1].text.strip())  # 페이지수 중 맨 뒤의 것 사용
        
        # Case: 페이지가 10장 초과 (기존처럼 맨 마지막 페이지로 이동해서 수집)
        else:
            # 접속
            page = 999  # 가능한 큰 페이지 (마지막 페이지로 가도록 하는 것)
            soup = self.get_link_page_soup(page)
            
            # 현재 페이지(=마지막 페이지) 가져오기
            if soup.select("#main_content > div.paging > strong"):
                last_page = soup.select("#main_content > div.paging > strong")[0].text
                last_page = int(last_page)
            
            # 위 방식으로도 가끔 page 값을 못 가져오는 경우가 있음, 이 경우 한 페이지씩 올라가면서 체크하도록
            else:
                joined_urls = ""
                
                if use_tqdm:
                    pbar = tqdm(range(1, page+1))
                else:
                    pbar = range(1, page+1)
                    
                for p in pbar:
                    if use_tqdm:
                        pbar.set_description("last_page 순차 탐색")
                    
                    hrefs = self.scrape_links_single_page(p)
                    jurls = "\n".join(hrefs)
                    
                    if jurls != joined_urls:
                        joined_urls = jurls
                    
                    else:  # 이전과 같을 경우 (해당 페이지 전 페이지가 마지막 페이지임)
                        last_page = p - 1
                        break

        return last_page
        
    
    def scrape_links_single_page(self, page=1):
        """날짜별 뉴스 목록의 한 페이지를 스크랩한다. 
        (특정 sid1, sid2의 특정 날짜의 특정 페이지 하나의 링크를 모두 받아온다)

        Args:
            page(int): 수집할 페이지의 숫자

        Returns:
            list: 해당 섹션 소분류 페이지에서 main_content 내에 속하는 모든 sid1이 일치하는 링크의 목록
                eg. ["url with sid1", "url with sid1", ...]
        """
        # soup 객체 생성
        soup = self.get_link_page_soup(page)
        
        # main_content의 내용만 가져오기
        main_content = soup.find("div", id="main_content")

        # 링크만 가져와서 필터링
        hrefs = [tag["href"] for tag in main_content.find_all("a")]
        hrefs = [href for href in hrefs if f"sid={self.sid1}" in href]  # sid가 sid1으로 잡히는 것 같음 (소분류는 args 내에는 없음)
        
        return hrefs

