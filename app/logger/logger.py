import os
import sys
# 현재 파일의 상위 디렉토리를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import psql




def log():
    """로깅 실행
    Args:

    """
    # TODO: 로깅 기능 여기에 추가, 일단 임시로 psql에 로깅