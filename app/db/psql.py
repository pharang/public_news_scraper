import psycopg2
import json
import os


############################
### 커넥션 & 커넥션 정보 ###


def find_config_db_json():
    """config.db.json 파일의 위치를 찾아 절대 경로로 반환한다

    config.db.json 파일의 형태
    {
        "DB별칭(eg. default)": {
            "DBMS": "postgresql",
            "HOST": host,  # 세부 내용은 db 유형마다 달라질 수 있음
            "DBNAME": dbname,
            "PORT": port,
            "USER": user,
            "PASSWORD": password
        },
        "DB별칭2": {  # 별칭으로 db를 구분 (postgresql이라던지 몽고용db 등등)
            ...
        },
        ...
    }
    """
    # 현재 스크립트의 절대 경로
    current_path = os.path.abspath(__file__)

    while True:
        # config.db.json 파일의 경로
        config_file = os.path.join(current_path, 'config.db.json')

        # 파일을 발견한 경우 종류
        if os.path.exists(config_file):
            target_json_path = os.path.join(current_path, "config.db.json")
            break

        # 루트 디렉토리에 도달한 경우 종료
        elif current_path == os.path.dirname(current_path):
            print('# config.db.json not exists #')
            print('# path:', target_json_path)
            target_json_path = None
            break

        # 없다면 상위 디렉토리로 이동
        else:
            current_path = os.path.dirname(current_path)

    return target_json_path
    


def connection_info(target_db="default"):
    """해당 명칭의 호스트의 접속 정보를 json파일로부터 가져온다.
    - 단일 DB만 사용할 경우 호스트는 default만 사용한다.
    - 서브 DB가 있을 경우 default 호스트를 유지하고, 서브 DB의 닉네임을 지정해 host를 추가한다.

    Args:
        target_db(str): 접속할 데이터베이스 별칭 (json파일에 저장된 명칭)
            | Default: "default" (메인 데이터베이스는 default로 통일하기)
    
    Returns:
        해당 호스트의 접속 정보
        host, dbname, port, user, password
    """
    # json path 찾아서 불러오기
    config_json = find_config_db_json()
    with open(config_json, "r") as f:
        json_loading = json.load(f)

    db_info = json_loading[target_db]
    host = db_info["HOST"]
    port = db_info["PORT"]
    dbname = db_info["DBNAME"]
    user = db_info["USER"]
    password = db_info["PASSWORD"]
    return host, port, dbname, user, password


def create_connection(target_db="default"):
    """connection_info로부터 접속 정보를 받아와 커넥션을 생성해 반환한다.

    Args:
        target_db(str): 접속할 데이터베이스 별칭 (json파일에 저장된 명칭)
    
    Returns:
        psycopg2 connection: 데이터베이스 커넥션
    """
    host, port, db, user, password = connection_info(target_db=target_db)
    connection = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
    return connection


#################
### 쿼리 실행 ###

def query_execute(query, data=()):
    """SELECT를 제외한 쿼리를 실행한다(결과 반환 없음)
    
    Args:
        query(str): 실행할 SQL 쿼리
        data(tuple): 쿼리를 execute할 때 넘겨줄 데이터 (= %s 자리에 들어갈 데이터)
    """
    conn = create_connection()
    cur = conn.cursor()
    cur.execute(query, data)
    conn.commit()
    conn.close()


def query_executemany(query, datas=()):
    """SELECT를 제외한 쿼리를 executemany로 실행한다(결과 반환 없음)
    
    Args:
        query(str): 실행할 SQL 쿼리
        datas(tuple): 쿼리를 execute할 때 넘겨줄 데이터 (= %s 자리에 들어갈 데이터)
    """
    conn = create_connection()
    cur = conn.cursor()
    cur.executemany(query, datas)
    conn.commit()
    conn.close()


def query_select(query, data=(), fetchone=False):
    """SELECT 쿼리를 실행해 결과를 반환한다

    Args:
        query(str): 실행할 SQL 쿼리
        data(tuple): 쿼리를 execute할 때 넘겨줄 데이터
        fetchone(boolean): fetahall이 아닌 fetchone으로 가져올 경우 True로 설정
    
    Return:
        list: fetchall()로 가져온 SELECT 쿼리 결과 데이터를 반환함
    """
    conn = create_connection()
    cur = conn.cursor()
    cur.execute(query, data)
    if fetchone:
        selected_datas = cur.fetchone()
    else:
        selected_datas = cur.fetchall()
    conn.close()
    return selected_datas




if __name__ == "__main__":
    # 직접 실행 시 db 커넥션 테스트
    print("## DB Connection Test ##")
    print("# Start #")
    print("Create Connection")
    conn = create_connection()
    print("Cursor")
    cur = conn.cursor()
    print("Close")
    conn.close()
    print("# Finished #")