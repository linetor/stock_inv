from mongodb import MongoManager
from stockCrawling import StockCrawling

if __name__ == "__main__":
    import pandas as pd
    stockInfor = StockCrawling()
    #print(stockInfor.insertCompanyInfor())
    #print(stockInfor.getCompanySize())
    #print(stockInfor.getStockByCode())
    print(stockInfor.insertDailyStockByCode(10))
    """
    url = 'http://finance.naver.com/item/sise_day.nhn?code={code}'.format(code=str(5930).zfill(6) )
    url = '{url}&page={page}'.format(url=url, page=1)
    print(url)
    df = pd.DataFrame()
    df = df.append(pd.read_html(url, header=0)[0], ignore_index=True)
    print(df)
    print(df.count())

    from bs4 import BeautifulSoup
    import requests
    result=requests.get(url)

    bs=BeautifulSoup(result.content,"html.parser")
    print("Beautiful",bs)
    salary = bs.find_all("td", class_="pgRR")[-1]  # last cell in the row
    print("salary",salary)
    print("salary : hred",salary.find_all('a', href=True)[-1]['href'])
    print("salary : href -1 ",salary.find_all('a', href=True)[-1]['href'].split("=")[-1])

    print("salary.text",salary.get_text())
    bs.find("p")
    """







