import win32com.client
import pythoncom
from configparser import ConfigParser

class XASessionEventHandler:
    login_state = 0
    def OnLogin(self, code, msg):
        if code == "0000":
            print("로그인 성공")
            XASessionEventHandler.login_state = 1
        else:
            print("로그인 실패")

instXASession = win32com.client.DispatchWithEvents("XA_Session.XASession", XASessionEventHandler)

configparser = ConfigParser()
configparser.read('.config')
id = configparser.get('logininfor', 'id')
passwd = configparser.get('logininfor', 'passwd')
cert_passwd = configparser.get('logininfor', 'cert_passwd')


instXASession.ConnectServer("hts.ebestsec.co.kr", 20001)
instXASession.Login(id, passwd, cert_passwd, 0, 0)

while XASessionEventHandler.login_state == 0:
    pythoncom.PumpWaitingMessages()