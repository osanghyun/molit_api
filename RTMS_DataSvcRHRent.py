"""
국토교통부_연립다세대 전월세 확정일자 자료 (연립다세대 전월세 신고정보 조회 기술문서.hwp 참조)
https://www.data.go.kr/data/15058016/openapi.do
요청변수 : 지역코드, 계약월
응답 메시지 명세 : 보증금액(만원), 월세금액(만원), 건축년도, 계약년도, 법정동, 연립다세대명, 계약월, 일, 전용면적(m^2), 지번, 지역코드, 층
"""


from molitBaseClass import *


class RTMS_DataSvcRHRent(setup):
    def __init__(self):
        super().__init__()
        self.url = "http://openapi.molit.go.kr:8081/\
                    OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcRHRent"
        self.tableName = "RTMS_DataSvcRHRent"
        self.dict_template = {"건축년도": "",
                              "년": "",
                              "법정동": "",
                              "보증금액": "",
                              "연립다세대": "",
                              "월": "",
                              "월세금액": "",
                              "일": "",
                              "전용면적": 0,
                              "지번": "",
                              "지역코드": "",
                              "층": ""}

    def insert_into_db(self, item_list):
        sql = 'INSERT INTO RTMS_DataSvcRHRent \
                (Build_Year, Deal_Year, Dong, Deposit, Apartment_Name, Deal_Month, Monthly_Rent, \
                Deal_Day, Area_for_Exclusive_Use, PyungArea, Jibun, Regional_Code, Floor, Hash) \
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        for item in item_list:
            item = self.get_sql_dict_item(item)
            hash_item = self.get_hash_str_from_tuple_item((item['건축년도'],
                                                           item['년'],
                                                           item['법정동'],
                                                           item['보증금액'],
                                                           item['월'],
                                                           item['월세금액'],
                                                           item['일'],
                                                           str(item['전용면적']),
                                                           item['지번'],
                                                           item['지역코드'],
                                                           item['층']))
            # 중복 시 건너뜀.
            if self.is_duplicated_value(hash_item):
                continue

            var = (str(item['건축년도']), str(item['년']), str(item['법정동']), str(item['보증금액']), str(item['연립다세대']),
                   str(item['월']), str(item['월세금액']), str(item['일']), float(item['전용면적']),
                   float(item['전용면적'])*0.3025, str(item['지번']), str(item['지역코드']), str(item['층']), hash_item)
            try:
                self.cur.execute(sql, var)
                self.con.commit()

            except Exception as e:
                print(str(e))
                return 1

        return 0


func = RTMS_DataSvcRHRent()

func.start()