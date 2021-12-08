"""
국토교통부_아파트 분양권전매 신고 자료 API
https://www.data.go.kr/data/15056782/openapi.do
"""
from molitBaseClass import *


class RTMS_DataSvcSilvTrade(setup):
    def __init__(self):
        super().__init__()
        self.url = 'http://openapi.molit.go.kr/\
                    OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcSilvTrade'
        self.tableName = "RTMS_DataSvcSilvTrade"
        self.dict_template = {
            '거래금액': "",
            '구분': "",
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

    def insert_into_db(self, item_list):  # 2 4 8
        sql = 'INSERT INTO RTMS_DataSvcSilvTrade (Deal_Amount, \
            Classification_of_Ownership, Deal_Year, Apartment_Name, Dong, \
            Sigungu, Deal_Month, Deal_Day, Area_for_Exclusive_Use, PyungArea,Jibun, Regional_Code,Floor, Hash) \
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

        for items in item_list:
            item = self.get_sql_dict_item(items)
            Hash_set = (item['년'], item['월'], item['일'], item['지번'], item['층'], item['지역코드'], item['거래금액'])
            # 년 월 일 지역코드 거래금액 지번 층
            Hash = super().get_hash_str_from_tuple_item(Hash_set)
            var = (item['거래금액'], item['구분'], item['년'], item['단지'], item['법정동'],
                   item['시군구'], item['월'],  item['일'], item['전용면적'], float(item['전용면적'])*0.3025,
                   item['지번'], item['지역코드'], item['층'], Hash)

            try:
                self.cur.execute(sql, var)
                self.con.commit()

            except Exception as e:
                print(str(e))
                return 1

        return 0


func = RTMS_DataSvcSilvTrade()
func.start()
