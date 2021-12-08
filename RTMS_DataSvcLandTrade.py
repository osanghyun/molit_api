"""
국토교통부_토지 매매 신고 조회 서비스
https://www.data.go.kr/data/15056649/openapi.do
"""
from molitBaseClass import *


class RTMS_DataSvcLandTrade(setup):
    def __init__(self):
        super().__init__()
        self.url = 'http://openapi.molit.go.kr/\
                    OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcLandTrade'
        self.tableName = "RTMS_DataSvcLandTrade"
        self.dict_template = {
            '거래금액': "",
            '거래면적': "",
            '구분': "",
            '년': "",
            '법정동': "",
            '시군구': "",
            '용도면적': "",
            '월': "",
            '일': "",
            '지목': "",
            '지역코드': ""}

    def insert_into_db(self, item_list):  # 2 4 6
        sql = 'INSERT INTO RTMS_DataSvcLandTrade (Deal_Amount, Deal_Area, \
            Classifications_of_Partial_Dealing, Deal_Year, Dong, Sigungu, \
            Zoning, Deal_Month, Deal_Day, Land_Use, Regional_Code, Hash) \
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

        for items in item_list:
            item = self.get_sql_dict_item(items)
            Hash_set = (item['년'], item['월'], item['일'], item['법정동'], item['거래금액'], item['지역코드'], item['법정동'])
            # 년 월 일 지역코드 거래금액 법정동 보증금액
            Hash = super().get_hash_str_from_tuple_item(Hash_set)
            var = (item['거래금액'], item['거래면적'], item['구분'], item['년'], item['법정동'],
                   item['시군구'], item['용도지역'], item['월'],  item['일'], item['지목'], item['지역코드'], Hash)

            try:
                self.cur.execute(sql, var)
                self.con.commit()

            except Exception as e:
                print(str(e))
                return 1

        return 0


func = RTMS_DataSvcLandTrade()
func.start()
