"""
국토교통부_오피스텔 매매 신고 조회 서비스
https://www.data.go.kr/data/15058452/openapi.do
"""
from molitBaseClass import *


class RTMS_DataSvcOffiTrade(setup):
    def __init__(self):
        super().__init__()
        self.url = 'http://openapi.molit.go.kr/\
                    OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcOffiTrade'
        self.tableName = "RTMS_DataSvcOffiTrade"
        self.dict_template = {
            '거래금액': "",
            '년': "",
            '단지': "",
            '법정동': "",
            '시군구': "",
            '월': "",
            '일': "",
            '전용면적': "",
            '지번': "",
            '지역코드': "",
            '층': ""}

    def insert_into_db(self, item_list):  # 13
        sql = 'INSERT INTO RTMS_DataSvcOffiTrade (Deal_Amount, Deal_Year, \
            Apartment_Name, Dong, Sigungu, Deal_Month, Deal_Day, \
            Area_for_Exclusive_Use, PyungArea, Jibun, Regional_Code, \
            Floor, Hash) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

        for items in item_list:
            item = self.get_sql_dict_item(items)
            Hash_set = (item['년'], item['월'], item['일'], item['지번'], item['지역코드'], item['층'], item['법정동'])
            # 년 월 일 지번 지역코드 층 법정동 보증금액
            Hash = super().get_hash_str_from_tuple_item(Hash_set)
            var = (item['거래금액'], item['년'], item['단지'], item['법정동'], item['시군구'],  item['월'],  item['일'],
                   item['전용면적'], float(item['전용면적'])*0.3025, item['지번'], item['지역코드'], item['층'], Hash)

            try:
                self.cur.execute(sql, var)
                self.con.commit()

            except Exception as e:
                print(str(e))
                return 1

        return 0


func = RTMS_DataSvcOffiTrade()
func.start()
