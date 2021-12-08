"""
국토교통부_상업업무용 부동산 매매 신고 자료 API
https://www.data.go.kr/data/15057267/openapi.do
"""
from molitBaseClass import *


class RTMS_DataSvcNrgTrade(setup):
    def __init__(self):
        super().__init__()
        self.url = 'http://openapi.molit.go.kr/\
                    OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcNrgTrade'
        self.tableName = "RTMS_DataSvcNrgTrade"
        self.dict_template = {
            '거래금액': "",
            '건물면적': 0,
            '건물주용도': "",
            '건축년도': "",
            '구분': "",
            '년': "",
            '대지면적': "",
            '법정동': "",
            '시군구': "",
            '용도지역': "",
            '월': "",
            '유형': "",
            '일': "",
            '지역코드': "",
            '층': ""}

    def insert_into_db(self, item_list):  # 2 4 7  3
        sql = 'INSERT INTO RTMS_DataSvcNrgTrade (Deal_Amount, Building_Area, \
            Building_Use, Build_Year,Classification_of_Share_Dealing, Deal_Year, \
            Plottage, Dong, Signgu, Land_Use, Deal_Month, Building_Type, Deal_Day, \
            Regional_Code, Floor,Hash) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

        for items in item_list:
            item = self.get_sql_dict_item(items)
            Hash_set = (item['년'], item['월'], item['일'], item['법정동'], item['층'], item['지역코드'], item['거래금액'])
            # 년 월 일 지역코드 거래금액 지번 층
            Hash = super().get_hash_str_from_tuple_item(Hash_set)
            var = (item['거래금액'], item['건물면적'], item['건물주용도'],
                   item['건축년도'], item['구분'], item['년'], item['대지면적'],
                   item['법정동'], item['시군구'], item['용도지역'], item['월'],
                   item['유형'], item['일'], item['지역코드'], item['층'], Hash)

            try:
                self.cur.execute(sql, var)
                self.con.commit()

            except Exception as e:
                print(str(e))
                return 1

        return 0


func = RTMS_DataSvcNrgTrade()
func.start()
