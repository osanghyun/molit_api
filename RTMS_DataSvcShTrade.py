"""
국토교통부_단독/다가구 매매 실거래 자료
https://www.data.go.kr/data/15058022/openapi.do
"""

from molitBaseClass import *


class RTMS_DataSvcShTrade(setup):
    def __init__(self):
        super().__init__()
        self.url = "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcSHTrade"
        self.tableName = "RTMS_DataSvcShTrade"
        self.dict_template = {"거래금액": "",
                              "건축년도": "",
                              "년": "",
                              "대지면적": "",
                              "법정동": "",
                              "연면적": "",
                              "월": "",
                              "일": "",
                              "주택유형": "",
                              "지역코드": ""}

    def insert_into_db(self, item_list):
        sql = 'INSERT INTO RTMS_DataSvcShTrade (Regional_Code, Deal_Year, Deal_Month, Deal_Day, Dong, Deal_Amount, House_Type, PyungArea, Plottage, Total_Floor_Area, Build_Year, Hash) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        for item in item_list:
            item = self.get_sql_dict_item(item)
            hash_item = self.get_hash_str_from_tuple_item((item['거래금액'],
                                                           item['건축년도'],
                                                           item['년'],
                                                           item['대지면적'],
                                                           item['법정동'],
                                                           item['연면적'],
                                                           item['월'],
                                                           item['일'],
                                                           item['주택유형'],
                                                           item['지역코드']))
            # 중복 시 건너뜀.
            if self.is_duplicated_value(hash_item):
                continue

            var = (str(item['지역코드']), str(item['년']), str(item['월']), str(item['일']), str(item['법정동']),
                   str(item['거래금액']), str(item['주택유형']), float(item['대지면적'])*0.3025, float(item['대지면적']), str(item['연면적']),
                   str(item['건축년도']), hash_item)
            try:
                self.cur.execute(sql, var)
                self.con.commit()

            except Exception as e:
                print(str(e))
                return 1
        return 0


func = RTMS_DataSvcShTrade()
func.start()