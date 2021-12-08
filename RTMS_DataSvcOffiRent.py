"""
국토교통부_오피스텔 전월세 신고 조회 서비스 API
https://www.data.go.kr/data/15059249/openapi.do
"""
from molitBaseClass import *


class RTMS_DataSvcOffiRent(setup):
    def __init__(self):
        super().__init__()
        self.url = 'http://openapi.molit.go.kr/\
                    OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcOffiRent'
        self.tableName = "RTMS_DataSvcOffiRent"
        self.dict_template = {
            '년': "",
            '단지': "",
            '법정동': "",
            '보증금': "",
            '시군구': "",
            '월': "",
            '월세': "",
            '일': "",
            '전용면적': "",
            '지번': "",
            '지역코드': "",
            '층': ""}

    def insert_into_db(self, item_list):
        sql = 'INSERT INTO RTMS_DataSvcOffiRent (Deal_Year, \
            Apartment_Name, Dong, Deposit, Sigungu, Deal_Month, Monthly_Rent, Deal_Day, \
            Area_for_Exclusive_Use, PyungArea, Jibun, Regional_Code, \
            Floor, Hash) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

        for items in item_list:
            item = self.get_sql_dict_item(items)
            Hash_set = (item['년'], item['월'], item['일'], item['지번'], item['지역코드'], item['층'], item['법정동'])
            # 년 월 일 지번 지역코드 층 법정동 보증금액
            Hash = super().get_hash_str_from_tuple_item(Hash_set)
            var = (item['년'], item['단지'], item['법정동'], item['보증금'], item['시군구'],  item['월'], item['월세'], item['일'],
                   item['전용면적'], float(item['전용면적'])*0.3025, item['지번'], item['지역코드'], item['층'], Hash)

            try:
                self.cur.execute(sql, var)
                self.con.commit()

            except Exception as e:
                print(str(e))
                return 1

        return 0


func = RTMS_DataSvcOffiRent()
func.start()
