import os
import sys
# 현재 파일의 상위 디렉토리를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import datetime

from db import psql



def get_kst_datetime():
    """한국 시간(UTC+9)으로 변환된 datetime을 반환 (타임존은 없이 사용하는 경우)"""
    kst_now = datetime.datetime.utcnow() + datetime.timedelta(hours=9)  # KST (UTC+9)
    return kst_now


def get_config(key, set_int=False):
    """info.config 에서 key로 value 값을 가져온다

    Args:
        key(str): kv로 가져올 값 중 key에 해당하는 값
        set_int(bool): 가져온 값을 int형으로 변환할지 여부
    """
    query = "SELECT value FROM info.config WHERE key = %s;"
    value = psql.query_select(query, (key,), fetchone=True)[0]
    if set_int:
        value = int(value)
    return value


def get_user_agent():
    """info.config 에서 User-Agent를 가져온다
    """
    query = "SELECT value FROM info.config WHERE key = %s;"
    raw = psql.query_select(query, ("user_agent",), fetchone=True)
    user_agent = raw[0]
    return user_agent


def get_years(order_by_desc=True):
    """info에서 전체 연도를 역순 정렬로 가져옴

    Args:
        order_by_desc(bool): True일 경우 역순정렬, False일 경우 순방향 정렬

    Returns:
        list: 연도의 리스트
    """
    query = "SELECT year FROM info.years;"
    raw = psql.query_select(query)
    years = [r[0] for r in raw]
    if order_by_desc:
        years = sorted(years, reverse=True)
    else:
        years = sorted(years, reverse=False)
    return years



def get_sids(sid1_only=False):
    """전체 sid1, sid2를 가져옮 (수집 여부 구분 없음)

    Args:
        sid1_only(bool): sid1만 반환할지 여부 | Default: False
            - False면 (sid1, sid2)의 리스트로
            - True면 sid1만 오름차순 정렬된 리스트로

    Returns:
        list: [(sid1, sid2), ...] or [sid1, ...]
    """
    query = "SELECT sid1, sid2 FROM info.sids;"
    sids = psql.query_select(query)
    return sids



def get_sid_name_mapper():
    """sid1, sid2의 코드에 이름을 매핑한 딕셔너리를 생성하여 반환
    - sid1, sid2의 조합(포함여부)는 고려하지 않고, 단순 이름 변환용

    Reutrns:
        dict: sid1에 sid1_name을 매핑한 딕셔너리
        dict: sid2에 sid2_name을 매핑한 딕셔너리
    """
    query = "SELECT sid1, sid1_name, sid2, sid2_name FROM info.sids;"
    raw = psql.query_select(query)

    sid1_name_mapper = {}
    sid2_name_mapper = {}

    for sid1, sid1_name, sid2, sid2_name in raw:
        sid1_name_mapper[sid1] = sid1_name
        sid2_name_mapper[sid2] = sid2_name

    return sid1_name_mapper, sid2_name_mapper