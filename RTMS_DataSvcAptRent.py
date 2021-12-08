"""
국토교통부_아파트 전월세 자료 API
https://www.data.go.kr/data/15058017/openapi.do
"""
from molitBaseClass import *


class RTMS_DataSvcAptRent(setup):
    def __init__(self):
        super().__init__()
        self.url = 'http://openapi.molit.go.kr:8081/\
                    OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptRent'
        self.tableName = "RTMS_DataSvcAptRent"
        self.dict_template = {'건축년도': "",
                              '년': "",
                              '법정동': "",
                              '보증금액': "",
                              '아파트': "",
                              '월': "",
                              '월세금액': "",
                              '일': "",
                              '전용면적': "",
                              '지번': "",
                              '지역코드': "",
                              '층': ""}

    def insert_into_db(self, item_list):
        sql = 'INSERT INTO RTMS_DataSvcAptRent (Build_Year,Deal_Year,\
            Dong, Deposit, Apartment_Name, Deal_Month, Monthly_Rent, \
            Deal_Day, Area_for_Exclusive_Use, PyungArea, Jibun, Regional_Code, Floor, Hash)\
             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

        for items in item_list:
            item = self.get_sql_dict_item(items)
            Hash_set = (item['년'], item['월'], item['일'], item['지번'], item['지역코드'], item['층'], item['법정동'])
            # 년 월 일 지번 지역코드 층 법정동 보증금액
            Hash = super().get_hash_str_from_tuple_item(Hash_set)
            var = (item['건축년도'], item['년'], item['법정동'], item['보증금액'],
                   item['아파트'], item['월'], item['월세금액'], item['일'],
                   item['전용면적'], float(item['전용면적'])*0.3025, item['지번'], item['지역코드'], item['층'], Hash)

            try:
                self.cur.execute(sql, var)
                self.con.commit()

            except Exception as e:
                print(str(e))
                return 1

        return 0


func = RTMS_DataSvcAptRent()
func.start()
