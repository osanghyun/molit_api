## 국토교통부 API
    Base Class : moltiBaseClass.py 의 setup Class

#### setup Class 구조
    PRE_STEP 1
    법정동 코드 리스트 : 법정동 테이블의 레코드를 받아 앞의 5자리를 중복없이 오름차순 정렬하여 저장.

    PRE_STEP 2
    계약년월 리스트 : 2017년 01월부터 2021년 01월까지 오름차순으로 저장.

    PRE_STEP 3
    시작점 설정 : 적재할 테이블에 저장된 레코드를 통해 가장 큰 법정동 코드의 가장 큰 계약년월로 설정.

    MAIN_STEP
    법정동 코드 리스트와 계약년월 리스트로 중첩된 반복문을 설정된 시작점부터 돌며 아래의 STEP 수행.
    아래의 STEP 수행 중 ERROR 발생 시 moltiLog 테이블에 테이블 이름, 법정동 코드, 계약년월 로깅.

        STEP 1.
        법정동 코드와 계약년월로 URL을 만들어 API 요청.

        STEP 2.
        API 응답메시지의 RESULT CODE로 부터 정상작동 판별.

        STEP 3.
        API 응답메시지에 명세값이 존재하는지 판별.

        STEP 4.
        응답 메시지 값 테이블에 저장.
        
    FINAL_STEP
    에러가 발생한 법정동 코드와 계약년월 값 API 재요청 후 디비에 적재.
    정상적으로 적재될 시 moltiLog 테이블에서 해당 값 삭제.
            

## Operations    
#### 1-1) 아파트 매매 실거래 자료
    
    명령어 : python3 RTMS_DataSvcAptTrade.py

    nsdi 데이터베이스 테이블 : RTMS_DataSvcAptTrade

    아파트 매매 신고정보 조회 기술문서.hwp 참조

#### 1-2) 아파트 매매 실거래 상세 자료
    
    명령어 : python3 RTMS_DataSvcAptTradeDev.py

    nsdi 데이터베이스 테이블 : RTMS_DataSvcAptTradeDev

    아파트 매매 상세자료 조회 기술문서.hwp 참조

#### 1-3) 아파트 전월세 자료
    
    명령어 : python3 RTMS_DataSvcAptRent.py

    nsdi 데이터베이스 테이블 : RTMS_DataSvcAptRent

    아파트 전월세 신고정보 조회 기술문서.hwp 참조

#### 1-4) 아파트 분양권전매 신고 자료
    
    명령어 : python3 RTMS_DataSvcSilvTrade.py

    nsdi 데이터베이스 테이블 : RTMS_DataSvcSilvTrade

    아파트 분양권전매 신고정보 조회 기술문서.hwp 참조

#### 2-1) 연립다세대 매매 실거래 자료
    
    명령어 : python3 RTMS_DataSvcRhTrade.py

    nsdi 데이터베이스 테이블 : RTMS_DataSvcRhTrade

    연립다세대 매매 신고정보 조회 기술문서.hwp 참조

#### 2-2) 연립다세대 전월세 자료

    명령어 : Python3 RTMS_DataSvcRHRent.py

    nsdi 데이터베이스 테이블 : RTMS_DataSvcRHRent
    
    연립다세대 전월세 신고정보 조회 기술문서.hwp 참조

#### 3-1) 단독다가구 매매 실거래 자료

    명령어 : Python3 RTMS_DataSvcShTrade.py

    nsdi 데이터베이스 테이블 : RTMS_DataSvcShTrade

    단독다가구 매매 신고정보 조회 기술문서.hwp 참조

#### 3-2) 단독다가구 전월세 자료

    명령어 : Python3 RTMS_DataSvcSHRent.py

    nsdi 데이터베이스 테이블 : RTMS_DataSvcSHRent

    단독다가구 전월세 신고정보 조회 기술문서.hwp 참조

#### 4-1) 오피스텔 매매 실거래 자료

    명령어 : Python3 RTMS_DataSvcOffiTrade.py

    nsdi 데이터베이스 테이블 : RTMS_DataSvcOffiTrade

    오피스텔 매매 신고정보 조회 기술문서.hwp 참조

#### 4-2) 오피스텔 전월세 자료

    명령어 : Python3 RTMS_DataSvcOffiRent.py

    nsdi 데이터베이스 테이블 : RTMS_DataSvcOffiRent

    오피스텔 전월세 신고정보 조회 기술문서.hwp 참조

#### 5) 토지 매매 신고 조회

    명령어 : Python3 RTMS_DataSvcLandTrade.py

    nsdi 데이터베이스 테이블 : RTMS_DataSvcLandTrade

    토지 매매 신고정보 조회 기술문서.hwp 참조

#### 6) 상업업무용 부동산 매매 자료

    명령어 : Python3 RTMS_DataSvcNrgTrade.py

    nsdi 데이터베이스 테이블 : RTMS_DataSvcLNrgTrade

    상업업무용 부동산 신고정보 조회 기술문서.hwp 참조

#### 국토교통부 API error logging

    nsdi 데이터베이스 테이블 : moltiLog
    
    API 요청, 데이터베이스 INSERT 시 발생한 Error에 대한 요청 메시지값 저장.