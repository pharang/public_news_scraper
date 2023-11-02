# 뉴스 스크래퍼 작업 코드

> 연구를 위한 뉴스 데이터를 수집하는 스크래퍼 제작 작업 코드 공유용 리포지토리입니다


### 구조

##### 폴더

- `app/`: 수집 프로그램 디렉토리
- `app/db/`: DB 관련 기능을 작성한 모듈
- `app/logger/`: 로깅 관련 기능을 작성할 모듈
- `app/queue_date_pages/`: 수집 대상 날짜-SID 조합을 추가하는 기능의 모듈
- `app/queue_news/`: 뉴스 큐를 추가하는 기능의 모듈
- `app/scraper_news/`: 큐를 기반으로 뉴스를 수집하는 기능의 모듈
- `app/utils/`: 프로그램 전반적으로 사용하는 유틸리티 기능들의 모듈

##### 파일

- `.config.db.json`: DB 접속 정보가 저장된 json 파일
- `Dockerfile.news`: 뉴스 스크래퍼 이미지 빌드를 위한 도커파일
- `Dockerfile.queue`: 큐 스크래퍼 이미지 빌드를 위한 도커파일
- `requirements.txt`: pip를 위한 requirements 정보

---


### 실행

- 2개의 도커파일을 통하여 news, queue 도커 이미지 생성 후 컨테이너로 실행합니다.
- 실행 전 config.db.json에 DB 접속을 위한 정보를 입력해주세요.
- 각 컨테이너는 n개 동시 실행이 가능한 매커니즘으로 제작되었습니다. 수집 속도 향상이 필요할 경우 동일한 이미지를 기반으로 컨테이너를 여러 개 추가하시면 됩니다.


---

### 기능

##### queue 컨테이너

- 수집 대상 date, sid 집합을 queue.date_pages 에 추가합니다.
- queue.date_pages에서 무작위로 가져온 정보를 기반으로 뉴스 페이지에 접속하여 뉴스 목록을 수집 후 queue.news 테이블에 저장합니다.

##### news 컨테이너

- queue.news 테이블에서 수집 대상 뉴스의 링크를 무작위로 가져옵니다
- 해당 링크로 접속 후 뉴스 정보를 수집하여 data.news 테이블에 저장합니다.
- 가장 최신 연도부터 내림차순으로 queue.news를 확인합니다.

---

### Database

#### info
##### info.years
| Column          | Dtype   | Constraint | Note                             |
| --------------- | ------- | ---------- | -------------------------------- |
| year            | INT     | PK         | 수집할 대상 연도들 (연도만 모음) |
| (미정) dates_queued | BOOLEAN |    | 해당 연도의 날짜가 모두 date queue에 추가됐는지 여부                                 |

##### info.sids
| Column    | Dtype | Constraint | Note                                  |
| --------- | ----- | ---------- | ------------------------------------- |
| sid1      | INT  | NOT NULL   | 섹션 아이디1                          |
| sid1_name | TEXT  | NOT NULL   | 섹션1 명칭                            |
| sid2      | INT  | PK         | 섹션 아이디2 (유니크하므로 PK) |
| sid2_name | TEXT  | NOT NULL   | 섹션2 명칭                            |
- 뉴스 수집을 위한 sid를 테이블로 별도로 저장 (추후 정치/사회 면 등의 다른 파트에 대한 정보를 수집하는 등으로 확장할 수 있도록)

##### info.config
| Column | Dtype | Constraint | Note                            |
| ------ | ----- | ---------- | ------------------------------- |
| key    | TEXT  | PK         | key-value에서 key에 해당하는 값 |
| value  | TEXT  |            | key에 매핑되는 value 값                                |
- 설정값들을 테이블로 저장 (key-value로 간단히 저장)


#### queue
##### queue.date_pages
| Column          | Dtype   | Constraint | Note                                             |
| --------------- | ------- | ---------- | ------------------------------------------------ |
| date_queue_id | TEXT    | PK         | `sid1-sid2-date` 순서로 큐 아이디 만들어 추가    |
| year            | INT     | NOT NULL   |                                                  |
| date            | TEXT    | NOT NULL   | 'yyyymmdd' 형태의 문자열                         |
| sid1            | INT     | NOT NULL   | 섹션1 아이디                                     |
| sid2            | INT     | NOT NULL   | 섹션2 아이디                                     |
| page_added        | BOOLEAN | NOT NULL   | 해당 페이지가 뉴스 큐에 수집하여 추가됐는지 여부 |
| page_length     | INT     |            | 큐 수집 후 총 페이지 수 저장 (UPDATE로)          |
| news_count      | INT     |            | 큐 수집 후 총 뉴스 개수 저장                     |
- 연도-날짜-sid1-sid2별 페이지 목록/수집여부, 이것 기반으로 페이지 방문해서 뉴스 큐 추가
- 개수가 수만개 이내로 제한될 예정이므로, 단일 테이블로 운영


##### queue.news_{연도}
| Column       | Dtype     | Constraint | Note                                                                                                 |
| ------------ | --------- | ---------- | ---------------------------------------------------------------------------------------------------- |
| news_year_id | SERIAL    | PK         | 연도별로 고유한 뉴스 아이디 (연도간에는 고유하지 않음)                                               |
| news_id      | BIGINT    | UNIQUE     | 연도 4자리 + news_year_id의 8자리까지(lzfill)를 합쳐 12자리로 만든 숫자 (모든 테이블에서 고유한 뉴스 아이디) |
| sid1         | INT       | NOT NULL   |                                                                                                      |
| sid2         | INT       | NOT NULL   |                                                                                                      |
| date         | TEXT      |            | 큐의 날짜                                                                                            |
| url          | TEXT      | NOT NULL   | 뉴스 링크                                                                                            |
| is_scraped   | BOOLEAN   | NOT NULL   | 해당 뉴스를 수집했는지 여부                                                                          |
| added        | TIMESTAMP |            | 큐에 추가된 시간 (KST)                                                                               |
| scraped      | TIMESTAMP |            | 수집된 시간 (KST)                                                                                    |
- 뉴스 수집 큐를 연도별로 분리하여 저장, 테이블명에 연도 포함 (뉴스 개수 문제로 인하여 연도별 한 테이블 사용함 / 개수를 대략 500만개 이내로 제한하는 것)


### data
##### data.news_{연도}

| Column       | Dtype     | Constraint | Note                                           |
| ------------ | --------- | ---------- | ---------------------------------------------- |
| news_year_id | INT       | PK         | 큐의 news_year_id와 같은 것                    |
| news_id      | BIGINT    | UNIQUE     | 큐의 news_id와 같은 것                         |
| sid1         | INT       |            | sid1, sid2, date는 queue와 같이                |
| sid2         | INT       |            |                                                |
| date         | TEXT      |            |                                                |
| page_url     | TEXT      |            | 접속 후 현재 페이지의 url을 다시 받아와서 저장 |
| scraped      | TIMESTAMP |            | 최초 수집된 시간                               |
| last_updated | TIMESTAMP |            | 수집 내용 업데이트한 시간                      |
| title        | TEXT      |            | 기사제목                                       |
| press        | TEXT      |            | 언론사                                         |
| input_date   | TIMESTAMP |            | 기사 입력날짜                                  |
| modify_date  | TIMESTAMP |            | 기사 수정날짜                                  |
| writer       | TEXT      |            | 기자(작성자)                                   |
| content      | TEXT      |            | 기사 본문 `<br>`을 줄바꿈으로 바꿔서 작성      |
| categories   | `TEXT[]`  |            | 기사 카테고리 목록                             |

- 연도별 뉴스 기사 수집하는 테이블

---

### 기타


#### 참고사항

- DB는 PostgreSQL을 사용하여 구현하였습니다.

##### TODO

- [ ]: 로깅 시스템 추가