"""
국토교통부 API Base Class [요청값이 법정동코드5자리와 계약년월로 이루어진 API 해당.]
nsdi DB의 table에 api 정보 저장을 위한 클래스.
db정보(setup.con)와 key(setup.key)값 입력 필요
"""
import pymysql
from datetime import datetime
from dateutil.relativedelta import relativedelta
from urllib.parse import quote_plus, urlencode
from urllib.request import urlopen, Request
import xmltodict
import logging
import sys
from tqdm import tqdm
import hashlib


class setup:
    def __init__(self):
        """
        DB 커넥터.
        API 서비스 키.
        기본 URL
        테이블 이름.
        """
        self.con = pymysql.connect(
            host='',  # 입력
            user='',  # 입력
            password='',  # 입력
            charset='utf8',
            db='',  # 입력
            cursorclass=pymysql.cursors.DictCursor)
        self.cur = self.con.cursor()
        self.key: str = ""  # API key값 입력 필요.
        self.url: str = ""
        self.tableName: str = ""
        self.dict_template: dict = {}

    def get_latest_db_value(self) -> tuple:
        """
        API 응답값을 저장할 테이블의 이미 저장되어 있는 값 중 가장 큰 법정동코드와 계약년월을 받아옴.
        :return: 법정동코드, 계약년월 시작점.
        """

        # STEP 1. 데이터베이스의 가장 큰 지역 코드값 받아옴.
        sql = "SELECT MAX(Regional_Code) FROM %(tableName)s"
        try:
            self.cur.execute(sql % {"tableName": self.tableName})
            item = self.cur.fetchall()
            Max_Regional_Code = item[0]['MAX(Regional_Code)']
        except Exception as e:  # 오류발생.
            print(str(e))
            logging.warning("(STEP 1)최근값 조회 중 오류가 발생했습니다. 프로그램을 종료합니다.")
            sys.exit()

        if not Max_Regional_Code:  # 데이터베이스가 비워져 있음.
            print("INSERT INTO EMPTY TABLE")
            return 0, 0

        # STEP 2. 데이터베이스의 가장 큰 지역 코드값을 가지는 Column 중 가장 큰 계약년도값 받아옴.
        sql = "SELECT MAX(Deal_Year) FROM %(tableName)s WHERE Regional_Code = %(Regional_Code)s"
        try:
            self.cur.execute(sql % {"tableName": self.tableName, "Regional_Code": Max_Regional_Code})
            item = self.cur.fetchall()
            Max_Deal_Year = item[0]['MAX(Deal_Year)']

        except Exception as e:  # 오류 발생.
            print(str(e))
            logging.warning("(STEP 2)최근값 조회 중 오류가 발생했습니다. 프로그램을 종료합니다.")
            sys.exit()

        if not Max_Deal_Year:  # 에러 발생.
            print("NO DATA")
            logging.warning("(STEP 2)최근값 조회 중 Max_Deal_Year 값이 존재하지 않습니다. 프로그램을 종료합니다.")
            sys.exit()

        # STEP 3. 데이터베이스의 가장 큰 지역 코드값과 가장 큰 계약년도값을 가지는 가장 큰 계약월값을 받아옴.
        sql = "SELECT MAX(CAST(Deal_Month AS DECIMAL(2))) \
               FROM %(tableName)s \
               WHERE Regional_Code = %(Regional_Code)s AND Deal_Year = %(Deal_Year)s"
        try:
            self.cur.execute(sql % {"tableName": self.tableName,
                                    "Regional_Code": Max_Regional_Code,
                                    "Deal_Year": Max_Deal_Year})
            item = self.cur.fetchall()
            Max_Deal_Month = str(item[0]['MAX(CAST(Deal_Month AS DECIMAL(2)))'])

        except Exception as e:  # 오류 발생.
            print(str(e))
            logging.warning("(STEP 3)최근값 조회 중 오류가 발생했습니다. 프로그램을 종료합니다.")
            sys.exit()

        if not Max_Deal_Month:  # 에러 발생.
            print("NO DATA")
            logging.warning("(STEP 3)최근값 조회 중 Max_Deal_Month 값이 존재하지 않습니다. 프로그램을 종료합니다.")
            sys.exit()

        if len(Max_Deal_Month) == 1:
            Max_Deal_Month = '0' + Max_Deal_Month

        print("CONTINUE INSERT INTO TABLE")
        return Max_Regional_Code, (Max_Deal_Year + Max_Deal_Month)

    def get_LAWD_CD_list_from_bubjungdongTable(self) -> list:
        """
        법정동테이블로부터 법정동코드값을 받아온다. -> 앞 5자리만 자른 뒤 중복을 제거하고 반환.
        :return: 법정동코드 앞 5자리 목록
        """
        print("LAWD_CD_list CALCULATING...")
        sql = "SELECT ldongCd FROM bubjungdong WHERE flag = 'Y'"
        try:
            self.cur.execute(sql)
            dict_ldcode = self.cur.fetchall()
        except Exception as e:
            print(str(e))
            sys.exit()

        list_ldcode = []

        for ldcode in dict_ldcode:
            list_ldcode.append(ldcode['ldongCd'][0:5])

        list_ldcode = list(set(list_ldcode))
        list_ldcode = sorted(list_ldcode)

        print("LAWD_CD_list SUCCESS")
        return list_ldcode

    def get_DEAL_YMD_list(self) -> list:
        """
        :return: 201701 부터 202101 까지의 날짜 목록.
        """
        print("DEAL_YMD_list CALCULATING...")
        startDate = '201701'

        DEAL_YMD_list = []

        while startDate < '202102':
            DEAL_YMD_list.append(startDate)
            startDate = (datetime.strptime(startDate, '%Y%m') + relativedelta(months=1)).strftime('%Y%m')

        print("DEAL_YMD_list SUCCESS")
        return DEAL_YMD_list

    def make_url(self, LAWD_CD: str, DEAL_YMD: str) -> str:
        """
        :param LAWD_CD: 법정동코드 앞 5자리.
        :param DEAL_YMD: 계약년월.
        :return: API 요청 URL에 요청 메시지를 합쳐서 반환.
        """
        queryParams = f'?{quote_plus("ServiceKey")}={self.key}&' + \
                      urlencode({quote_plus('LAWD_CD'): LAWD_CD,
                                 quote_plus('DEAL_YMD'): DEAL_YMD})

        return self.url + queryParams


    def insert_into_db(self, dict_list: list) -> int:
        """
        API 응답값 테이블에 저장.
        :param dict_list: API 요청에 대한 응답값들.
        :return: Error 발생 유무 반환.
        """
        pass

    def error_logging_into_moltiLog_table(self, Dong: str, Deal_ym: str) -> None:
        """
        Error가 발생한 테이블이름, 법정동코드, 계약년월을 moltiLog Table에 저장.
        :param Dong:
        :param Deal_ym:
        :return:
        """
        sql = "INSERT INTO moltiLog (TableName, Dong, Deal_ym) VALUES (%s,%s,%s)"
        var = (self.tableName, Dong, Deal_ym)
        try:
            self.cur.execute(sql, var)
            self.con.commit()
        except Exception as e:
            print(str(e))
            logging.warning("WARNING : %s %s %s", self.tableName, Dong, Deal_ym)

    def get_sql_dict_item(self, dict_item: dict) -> dict:
        """
        누락된 API 응답 딕셔너리값을 위한 코드.
        :param dict_item:
        :return:
        """
        dict_template_copy: dict = self.dict_template.copy()
        try:
            for key in dict_item.keys():
                if dict_item[str(key)] == None:
                    continue
                dict_template_copy[str(key)] = dict_item[str(key)]
        except Exception as e:
            print(str(e))
            print("get_sql_dict_item 오류")
            sys.exit()
        return dict_template_copy

    def get_hash_str_from_tuple_item(self, tuple_item: tuple) -> str:
        """
        tuple의 값들을 조합하여 해쉬값 반환.
        :param tuple_item: 해싱에 사용될 값들.
        :return: 해쉬값.
        """
        str_item: str = ""
        item: str
        for item in tuple_item:
            str_item += item
        return hashlib.sha1(str_item.encode()).hexdigest()

    def is_duplicated_value(self, hash_item: str) -> int:
        """
        해쉬값을 통해 중복여부 판별 (중복 O: 1 / 중복 X: 0)
        :param hash_item: 해쉬값
        :return: 중복여부
        """
        sql = "SELECT * FROM %(tableName)s WHERE Hash = '%(Hash_Value)s'"
        try:
            self.cur.execute(sql % {'tableName': self.tableName, 'Hash_Value': hash_item})
            isDuplicated = len(self.cur.fetchall())
        except Exception as e:
            print(str(e))
            sys.exit()

        return isDuplicated

    def request_api_and_insert_into_db(self, LAWD_CD_list:list, DEAL_YMD_list:list,
                                       start_LAWD_CD:str, start_DEAL_YMD:str) -> None:
        """
        법정동 코드 리스트, 계약년월 리스트를 받아 API 요청을 위한 URL을 생성하고 API 응답값을 DB에 저장.
        :param LAWD_CD_list: 법정동코드 앞 5자리 목록.
        :param DEAL_YMD_list: 계약년월 목록.
        :param start_LAWD_CD: 법정동코드 앞 5자리 시작지점.
        :param start_DEAL_YMD: 계약년월 시작지점.
        :return:
        """
        print("request_api_and_insert_into_db CALCULATING...")
        for LAWD_CD in tqdm(LAWD_CD_list, total=len(LAWD_CD_list)):
            # 재시작 시 디비에 저장된 마지막 값부터 API 요청 시작.
            if int(LAWD_CD) < int(start_LAWD_CD):
                continue

            for DEAL_YMD in tqdm(DEAL_YMD_list, total=len(DEAL_YMD_list)):
                # 재시작 시 디비에 저장된 마지막 값부터 API 요청 시작.
                if int(LAWD_CD) == int(start_LAWD_CD) and int(DEAL_YMD) < int(start_DEAL_YMD):
                    continue

                url = self.make_url(LAWD_CD, DEAL_YMD)

                # STEP 1. url request -> xml to dict.
                try:
                    response = Request(url)
                    response.get_method = lambda: 'GET'
                    response_body = urlopen(response).read()
                    my_dict = xmltodict.parse(response_body)
                except Exception as e:  # API 요청에 문제가 생겼을 때.
                    print(str(e))
                    self.error_logging_into_moltiLog_table(LAWD_CD, DEAL_YMD)
                    continue

                # STEP 2. Distinguishing api result.
                resultCode = my_dict['response']['header']['resultCode']
                if resultCode != '00':  # 서버가 비정상일 때.
                    print("resultCodeError")
                    self.error_logging_into_moltiLog_table(LAWD_CD, DEAL_YMD)
                    continue

                # STEP 3. 요청 메시지에 대한 응답 값이 존재하는 지 판별.
                items = my_dict['response']['body']['items']
                if not items:  # 응답 값이 존재하지 않을 때.
                    continue

                # STEP 4. 응답 메시지 값 디비에 저장.
                item_list = items['item']
                if type(item_list) is list:
                    isError = self.insert_into_db(item_list)
                else:  # 응답 메시지 값이 하나일 경우 dict_list가 아닌 dict로 들어옴.
                    isError = self.insert_into_db([item_list, ])
                if isError:  # 데이터베이스에 문제가 생겼을 때.
                    self.error_logging_into_moltiLog_table(LAWD_CD, DEAL_YMD)
                    continue

        print("request_api_and_insert_into_db SUCCESS")

    def get_error_logging_items(self) -> list:
        """
        moltilog Table에 저장된 error logging값 받아옴.
        :return: 법정도코드 앞 5자리, 계약년월 목록.
        """
        sql = "SELECT Dong, Deal_ym FROM moltiLog WHERE TableName = '%(table)s'"
        try:
            self.cur.execute(sql % {'table': self.tableName})
            items = self.cur.fetchall()
        except Exception as e:
            print(str(e))
            sys.exit()
        return items

    def request_api_and_insert_into_db_for_error_items(self, items) -> None:
        """
        error값 디비에 저장.
        :param items: dict_list 또는 dict
        :return:
        """
        print("request_api_and_insert_into_db_error_items CALCULATING...")
        for item in items:
            LAWD_CD = item['Dong']
            DEAL_YMD = item['Deal_ym']

            url = self.make_url(LAWD_CD, DEAL_YMD)

            # STEP 1. url request -> xml to dict.
            try:
                response = Request(url)
                response.get_method = lambda: 'GET'
                response_body = urlopen(response).read()
                my_dict = xmltodict.parse(response_body)
            except Exception as e:  # API 요청에 문제가 생겼을 때.
                print(str(e))
                self.error_logging_into_moltiLog_table(LAWD_CD, DEAL_YMD)
                continue

            # STEP 2. Distinguishing api result.
            resultCode = my_dict['response']['header']['resultCode']
            if resultCode != '00':  # 서버가 비정상일 때.
                print("resultCodeError")
                self.error_logging_into_moltiLog_table(LAWD_CD, DEAL_YMD)
                continue

            # STEP 3. 요청 메시지에 대한 응답 값이 존재하는 지 판별.
            items = my_dict['response']['body']['items']
            if not items:  # 응답 값이 존재하지 않을 때.
                continue

            # STEP 4. 응답 메시지 값 디비에 저장.
            item_list = items['item']
            if type(item_list) is list:
                isError = self.insert_into_db(item_list)
            else:  # 응답 메시지 값이 하나일 경우 dict_list가 아닌 dict로 들어옴.
                isError = self.insert_into_db([item_list, ])
            if isError:  # 데이터베이스에 문제가 생겼을 때.
                self.error_logging_into_moltiLog_table(LAWD_CD, DEAL_YMD)
                continue

            # STEP 5. Error Table의 값 삭제.
            sql = "DELETE FROM moltiLog WHERE TableName = '%(table)s' AND Dong = %(dong)s AND Deal_ym = %(deal_ym)s"
            try:
                self.cur.execute(sql % {'table': self.tableName, 'dong': LAWD_CD, 'deal_ym': DEAL_YMD})
                self.con.commit()
            except Exception as e:
                print(str(e))
                sys.exit()
        print("request_api_and_insert_into_db_error_items SUCCESS")

    def start(self):
        """
        1. 법정동 코드 앞 5자리, 계약년월의 시작지점을 받아온다.
        2. 법정동 코드 앞 5자리 목록을 받아온다.
        3. 계약년월 목록을 받아온다.
        4. 목록에 대한 API 요청 후 테이블에 저장.
        5. 테이블에 저장 중 발생한 Error에 대한 처리.
        :return:
        """
        start_LAWD_CD, start_DEAL_YMD = self.get_latest_db_value()

        # 법정동 코드 리스트.
        LAWD_CD_list = self.get_LAWD_CD_list_from_bubjungdongTable()

        # 계약년월 리스트.
        DEAL_YMD_list = self.get_DEAL_YMD_list()

        # API 요청 후 응답값 디비에 저장.
        self.request_api_and_insert_into_db(LAWD_CD_list, DEAL_YMD_list, start_LAWD_CD, start_DEAL_YMD)

        # Error 값 디비에 저장.
        items = self.get_error_logging_items()
        self.request_api_and_insert_into_db_for_error_items(items)

        print("DONE")