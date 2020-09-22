from mongodb import MongoManager
from stockCrawling import StockCrawling

if __name__ == "__main__":
    stockInfor = StockCrawling()
    print(stockInfor.insertCompanyInfor())
    print(stockInfor.getCompanySize())