"""
국토교통부_단독/다가구 전월세 자료 (단독다가구 전월세 신고정보 조회 기술문서.hwp)
https://www.data.go.kr/data/15058352/openapi.do
"""
from molitBaseClass import *


class RTMS_DataSvcSHRent(setup):
    def __init__(self):
        super().__init__()
        self.url = "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcSHRent"
        self.tableName = "RTMS_DataSvcSHRent"
        self.dict_template = {"지역코드": "",
                              "년": "",
                              "월": "",
                              "일": "",
                              "법정동": "",
                              "보증금액": "",
                              "월세금액": "",
                              "건축년도": ""}

    def insert_into_db(self, item_list):
        sql = 'INSERT INTO RTMS_DataSvcSHRent (Regional_Code, Deal_Year, Deal_Month, Deal_Day, Dong, Deposit, Monthly_Rent, Build_Year, Hash) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
        for item in item_list:
            item = self.get_sql_dict_item(item)
            hash_item = self.get_hash_str_from_tuple_item((item['지역코드'],
                                                           item['년'],
                                                           item['월'],
                                                           item['일'],
                                                           item['법정동'],
                                                           item['보증금액'],
                                                           item['월세금액'],
                                                           item['건축년도']))
            # 중복 시 건너뜀.
            if self.is_duplicated_value(hash_item):
                continue

            var = (str(item['지역코드']), str(item['년']), str(item['월']), str(item['일']), str(item['법정동']),
                   str(item['보증금액']), str(item['월세금액']), str(item['건축년도']), hash_item)
            try:
                self.cur.execute(sql, var)
                self.con.commit()

            except Exception as e:
                print(str(e))
                return 1
        return 0


func = RTMS_DataSvcSHRent()
func.start()