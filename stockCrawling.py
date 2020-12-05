from mongodb import MongoManager
import pandas as pd
import json

class StockCrawling:
    mongoclient = None
    _companyinforrename_= {'회사명': 'company', '종목코드': 'code','업종':'business','주요제품':'main'
        ,'상장일':'startdate' ,'결산월':'statemonth','대표자명':'owner','홈페이지':'homepage','지역':'loc'}

    def __init__(self):
        if self.mongoclient == None:
            self.mongoclient = MongoManager.getInstance().stock_kor

        def __del__(self):
            self.mongoclient.close()

    def getCompanyInfor(self):
        stock_code = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download', header=0)[0]
        stock_code = stock_code.rename(columns= self._companyinforrename_)
        stock_code ['_id'] = stock_code['code']
        # 종목코드가 6자리이기 때문에 6자리를 맞춰주기 위해 설정해줌
        # stock_code.code = stock_code.code.map('{:06d}'.format)
        return stock_code

    def insertCompanyInfor(self):
        stock_code = self.getCompanyInfor()
        ids = json.loads(stock_code['_id'].T.to_json()).values()
        records = json.loads(stock_code.T.to_json()).values()
        #print(stock_code.T.to_json())
        #print(records)

        from collections import defaultdict
        returnDic = defaultdict(list)
        for x in records:
            tempValue=self.mongoclient.companyinfor.update({"_id":x["_id"]},x,upsert=True)
            for k, v in tempValue.items():
                returnDic[k].append(v)
        returnValue = {}
        from functools import reduce
        import numbers

        for x in returnDic.keys():
            if(isinstance(returnDic[x][0], numbers.Number)):
                returnValue[x]= reduce(lambda x,y: x + y,returnDic[x],0)
            elif type(x) == type(True) :
                returnValue[x] = len(returnDic[x])
            else:
                returnValue[x] = returnDic[x]

        return (returnValue)

    def getCompanySize(self):
        return self.mongoclient.companyinfor.count()

    def insertDailyStockByCode(self,codeNum):

        getStockInfor = self.mongoclient.companyinfor.find_one({ "code": codeNum })
        print(getStockInfor)
        if(getStockInfor == None):
            return "code:"+str(codeNum)+" not valid"

        import pymongo
        getStockDailyLastDate = self.mongoclient.dailystockprice.find_one ( { "code" : codeNum},sort=[("date", pymongo.DESCENDING)])

        df = pd.DataFrame()
        urlOriginal = 'http://finance.naver.com/item/sise_day.nhn?code={code}'.format(code= str(codeNum).zfill(6))
        url = '{url}&page={page}'.format(url=urlOriginal, page=1)

        lastdate = '1900-01-01'
        pageNum = 0
        from bs4 import BeautifulSoup
        import requests
        print(getStockDailyLastDate)


        if(getStockDailyLastDate != None ):
            print("if condition")
            import datetime
            from datetime import date
            import numpy as np
            print("Not None",getStockDailyLastDate)
            lastdate = datetime.datetime.fromtimestamp(getStockDailyLastDate['date']/1000 + 60*60*24).strftime('%Y-%m-%d')
            curdate = date.today().strftime("%Y-%m-%d")
            print("checking date",lastdate,curdate)
            weekdaydiff = np.busday_count(lastdate, curdate)
            print("date diff",weekdaydiff)
            if weekdaydiff > 0 :
                pageNum = (weekdaydiff-1) // 10 + 1

        else :
            print("else condition")
            result=requests.get(url)
            bs = BeautifulSoup(result.content,"html.parser")
            listChecking=bs.find_all("td", class_="pgRR")
            if(len(listChecking)==0):
                return "code:"+str(codeNum)+" not recorded"
            pageNum = int(bs.find_all("td", class_="pgRR")[-1].find_all('a', href=True)[-1]['href'].split("=")[-1])

        print("page check",pageNum)
        import time
        import random
        for pageNum in range(1,pageNum+1):
            url = '{url}&page={page}'.format(url=urlOriginal, page=pageNum)
            df = df.append(pd.read_html(url, header=0)[0].dropna(axis=0), ignore_index=True)
            sleepTime = (1000 + round(random.uniform(-1, 1) * 500))/1000
            print(pageNum,"sleep",sleepTime)
            time.sleep(sleepTime)


        print("df count check" , df.size )

        if len(df) > 0 :
            df = df.rename(columns= {'날짜': 'date', '종가': 'close', '전일비': 'diff', '시가': 'open', '고가': 'high', '저가': 'low', '거래량': 'volume'})

            df[['close', 'diff', 'open', 'high', 'low', 'volume']] = df[['close', 'diff', 'open', 'high', 'low', 'volume']].astype(int)
            df['strdate'] = df['date']
            df = df[df['strdate'] > lastdate]
            df['date'] = pd.to_datetime(df['date'])
            df['code'] = codeNum
            df = df.sort_values(by=['date'], ascending=True)
            df = df.reset_index(drop=True)

            print("df",df)
            print("df.count",df.count())
            records = json.loads(df.T.to_json()).values()
            self.mongoclient.dailystockprice.insert(records)

        return "code:"+str(codeNum)+" insert : "+ str(len(df))
        # to do
    #self.mongoclient.dailystockprice.count()
