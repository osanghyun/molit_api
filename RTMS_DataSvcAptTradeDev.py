"""
국토교통부_아파트매매 실거래 상세 자료 API
https://www.data.go.kr/data/15057511/openapi.do
"""
from molitBaseClass import *


class RTMS_DataSvcAptTradeDev(setup):
    def __init__(self):
        super().__init__()
        self.url = 'http://openapi.molit.go.kr/\
                    OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev'
        self.tableName = "RTMS_DataSvcAptTradeDev"
        self.dict_template = {
            '거래금액': "",
            '건축년도': "",
            '년': "",
            '도로명': "",
            '도로명건물본번호코드': "",
            '도로명건물부번호코드': "",
            '도로명시군구코드': "",
            '도로명일련번호코드': "",
            '도로명지상지하코드': "",
            '도로명코드': "",
            '법정동': "",
            '법정동본번코드': "",
            '법정동부번코드': "",
            '법정동시군구코드': "",
            '법정동읍면동코드': "",
            '법정동지번코드': "",
            '아파트': "",
            '월': "",
            '일': "",
            '전용면적': "",
            '지번': "",
            '지역코드': "",
            '층': ""}

    def insert_into_db(self, item_list):  # 2 5 7 6 5
        sql = 'INSERT INTO RTMS_DataSvcAptTradeDev (Deal_Amount, Build_Year, \
            Deal_Year, Road_Name, Road_Name_Bonbun, Road_Name_Bubun, Road_Name_Sigungu_Code, \
            Road_Name_Seq, Road_Name_Basement_Code, Road_Name_Code, Dong, Bonbun, Bubun, Sigungu_Code, \
            Eubmyundong_Code, Land_Code, Apartment_Name, Deal_Month, Deal_Day, Area_for_Exclusive_Use, \
            PyungArea, Jibun, Regional_Code, Floor, Hash) VALUES \
            (%s,%s,%s,%s,%s,%s,%s ,%s,%s,%s,%s,%s,%s,%s ,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s)'

        for items in item_list:
            item = self.get_sql_dict_item(items)
            Hash_set = (item['거래금액'], item['년'], item['월'], item['일'],
                        item['지번'], item['지역코드'], item['층'], item['법정동'])
            # 거래금액 년 월 일 지번 지역코드 층 법정동 보증금액
            Hash = super().get_hash_str_from_tuple_item(Hash_set)
            var = (item['거래금액'], item['건축년도'], item['년'], item['도로명'], item['도로명건물본번호코드'],
                   item['도로명건물부번호코드'], item['도로명시군구코드'], item['도로명일련번호코드'],
                   item['도로명지상지하코드'], item['도로명코드'], item['법정동'], item['법정동본번코드'],
                   item['법정동부번코드'], item['법정동시군구코드'], item['법정동읍면동코드'], item['법정동지번코드'],
                   item['아파트'], item['월'], item['일'], item['전용면적'], float(item['전용면적'])*0.3025,
                   item['지번'], item['지역코드'], item['층'], Hash)

            try:
                self.cur.execute(sql, var)
                self.con.commit()

            except Exception as e:
                print(str(e))
                return 1

        return 0


func = RTMS_DataSvcAptTradeDev()
func.start()
