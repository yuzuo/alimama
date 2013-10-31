# -*- coding:utf-8 -*-
import __future__
import datetime
import time
import MySQLdb
import re
from cookielib import LWPCookieJar
import urllib2
import hashlib
import urllib
from BeautifulSoup import BeautifulSoup
import logging
import os
import xlrd
from email.mime.multipart import MIMEMultipart
import smtplib
from email.mime.text import MIMEText

"""
爬虫步骤分析：
1：
爬取数据作为底层历史数据(tbk_detail_history)
    判断订单状态
        订单结算:设置其结算价格为当前价格，
        订单未结算:判断该商品是否已经存在到当前的数据表中
爬取最新数据1个月
    删除一个月的最新数据(tbk_detail)
    判断时间：
        不存在：获取历史数据中的价格为当前价格(current_price)
        存在:抓取实时价格的数据
"""
#DAEMON_HOST = "192.168.0.74"
#DAEMON_PORT = 19999
USER_AGENT = "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)"
MYSQL_HOST = '192.168.0.68'
MYSQL_PORT = 3306
MYSQL_DB = 'analyzedb'
MYSQL_USER = 'analyze'
MYSQL_PASSWD = 'xcLQ6HCNl5njyV'
MYSQL_CHARSET = 'utf8'
#USER_AGENT = "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)"
##MYSQL_HOST = '10.1.2.43'
#MYSQL_HOST = 'localhost'
#MYSQL_PORT = 3306
#MYSQL_DB = 'analyzedb'
#MYSQL_USER = 'root'
#MYSQL_PASSWD = ''
#MYSQL_CHARSET = 'utf8'


MAIL_HOST = "stmphost"
MAIL_USER = "mailuser"
MAIL_PASSWD = "mailpasswd"

#BASE_MEDIA = "E:/WORKSPACE/Guang/analyze/media/upload"
BASE_MEDIA = "../media/upload"


#===============================================================================
# 删除重复时间的记录
#===============================================================================
def delete_tbk_detail(conn,table_name,start,end):
    query = """
    delete from `%s` where create_time between '%s' and '%s 23:59:59'
    """%(table_name,start,end)
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
#===============================================================================
# 发送邮件
#===============================================================================

def send_mail(toaddrs, subject, text):
    loginUser = 'login@email.com'
    loginPassword = 'email_username'
    mailHost="smtp.emaill.com"
    mailFrom = "<"+loginUser+">"
    message = MIMEText(text)
    message['From'] = 'email_from'
    message['To'] = toaddrs
    message['Subject'] = subject

    mailServer = smtplib.SMTP()
    mailServer.connect(mailHost)
    mailServer.login(loginUser,loginPassword)
    mailServer.sendmail(mailFrom, toaddrs, message.as_string())
    mailServer.close()


    

#===============================================================================
# 配置logging
#===============================================================================
def init_logging():
    PATH = os.path.realpath(os.path.dirname(__file__))
    logging.basicConfig(
        filename=os.path.join(PATH, 'tbk.log'),
        filemode='a',
        format="%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO)
    return logging

def get_now():
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return now

#===============================================================================
# 获取当前该产品的价格
#===============================================================================

def getOneMonthAgo():
    today = datetime.datetime.now()
    yesterday = getYesterDay()
    oneMonthAgo = today - datetime.timedelta(days=62)
    return oneMonthAgo.strftime("%Y-%m-%d"),yesterday

def getYesterDay():
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    return yesterday

#===============================================================================
# 这里可以设置多个登陆账户
#===============================================================================
login_info = [{"logname":"my_logname",
            "originalLogpasswd":"my_originalLogpasswd",
            "login_name":u"mylogin_name"}]
#===============================================================================
# 登陆验证
#===============================================================================
def get_tb_token(cj):
    tb_token = ""
    for item in cj:
        if item.name == "_tb_token_":
            tb_token = item.value
        break
    return tb_token

    
#===============================================================================
# excel到 mysql
#===============================================================================
def tbk_excel(conn,excelpath,user_id,table_name):
    conn.set_character_set('utf8')
    cursor = conn.cursor()
    cursor.execute("set names utf8")
    if os.path.exists(excelpath):
        wb = xlrd.open_workbook(excelpath,encoding_override='utf-8')
        sh = wb.sheet_by_index(0)
        if int(sh.nrows)>1:
            for rownum in range(1, sh.nrows):
                create_time = sh.row_values(rownum)[0]#创建时间date
                item_title = sh.row_values(rownum)[1]#商品信息
                item_num = sh.row_values(rownum)[2]#商品数
                price = sh.row_values(rownum)[3]#商品单价
                status = sh.row_values(rownum)[4]#订单状态
                pay_time = sh.row_values(rownum)[5]#结算时间
                deal_price = sh.row_values(rownum)[6]#实际成交价格
                commission_rate = sh.row_values(rownum)[7]#佣金比率
                if commission_rate.endswith("%"):
                    commission_rate = float(commission_rate.rpartition('%')[0]) * 0.01
                thirdpart_rate = sh.row_values(rownum)[8]#第三方服务费率 
                if thirdpart_rate.endswith("%"):
                    thirdpart_rate = float(thirdpart_rate.rpartition('%')[0]) * 0.01
                pre_commission = sh.row_values(rownum)[9]#预计佣金收益
                product_id = sh.row_values(rownum)[10]#商品ID
                seller_nick = sh.row_values(rownum)[11]#掌柜旺旺
                shop_title = sh.row_values(rownum)[12]#所属店铺
                #商品佣金收益
                #天猫补贴比率
                #天猫补贴金额
                #是否第三方分成
                #第三方服务费来源
                #服务费
                order_number = sh.row_values(rownum)[19]#订单编号
                #===========================================================
                # 插入数据
                #===========================================================
                query_ins = """
                INSERT INTO `%s` (`create_time`, `item_title`, `seller_nick`, `shop_title`, `item_num`, `price`, `status`, `pay_time`, `deal_price`, `commission_rate`, `thirdpart_rate`, `pre_commission`, `product_id`, `order_number`, `user_id`, `status_new`) 
                VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');
                """%(table_name, create_time, item_title.replace('%','%%'), seller_nick, shop_title, item_num, price, status, pay_time, deal_price, commission_rate, thirdpart_rate, pre_commission, product_id, order_number,  user_id, status)
                logging.info(query_ins)
                try:
                    cursor = conn.cursor()
                    cursor.execute(query_ins)
                    conn.commit()
                except MySQLdb.Error,e:
                    try:
                        error_msg = "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
                    except IndexError:
                        error_msg = "MySQL Error: %s" % str(e)
                    logging.info(error_msg)
                    send_mail('email@address', 'MYSQL ERROR', error_msg)
                    pass
    else:
        return False
    

##===============================================================================
## 下载文件保存
##===============================================================================
def download_excel(postdata,headers,user_id,start,end,download_url):
    req2 = urllib2.Request(
                  url=download_url,
                  data=postdata,
                  headers=headers
                  )
    f = urllib2.urlopen(req2)
    filename = f.info()['Content-Disposition'].split('filename=')[1]
    file_instance = open(filename,'wb')
    file_instance.write(f.read())
    file_instance.close()
    dest_dir = os.path.join(BASE_MEDIA)
    if os.path.isdir(dest_dir) and os.path.exists(dest_dir):
        pass
    else:
        os.makedirs(dest_dir)
    dest_name = os.path.join(dest_dir,str(user_id)+".xls").replace('\\','/')
    if os.path.exists(dest_name):
        os.remove(dest_name)
    os.rename(filename,"%s" %(dest_name))
    return dest_name
#===============================================================================
# 订单
#===============================================================================

def spider_tbk_list(start, end, logname, originalLogpasswd,login_name,user_id):
    login_url = "http://www.alimama.com/member/minilogin.htm?&proxy=http://www.alimama.com/proxy.htm"
    post_url = "http://www.alimama.com/member/minilogin_act.htm"
    
    cj = LWPCookieJar()
    cookie_support = urllib2.HTTPCookieProcessor(cj)  
    opener = urllib2.build_opener(cookie_support, urllib2.HTTPHandler)  
    urllib2.install_opener(opener)
    urllib2.urlopen(login_url)
    tb_token = get_tb_token(cj)
    
    headers = {
               "HOST":'www.alimama.com',
               'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6',
               "Referer":"http://www.alimama.com/member/minilogin.htm?&proxy=http://www.alimama.com/proxy.htm",
               "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
               "ContentType":"application/x-www-form-urlencoded",
    }
    
    download_url = "http://u.alimama.com/union/newreport/taobaokeDetail.do?startTime=%s&endTime=%s&DownloadID=DOWNLOAD_REPORT_INCOME_NEW" %(start,end)
    logpasswd = hashlib.md5(originalLogpasswd).hexdigest()
    postdata = urllib.urlencode({
                               "_tb_token_":"%s" % (tb_token),
                               "style":"",
                               "redirect":download_url,
                               "proxy":"http://u.alimama.com/union/proxy.htm",
                               "logname":'%s' % logname,
                               "originalLogpasswd":"%s" % originalLogpasswd,
                               "logpasswd":"%s" % logpasswd,
                             })
    req = urllib2.Request(
                      url=post_url,
                      data=postdata,
                      headers=headers
                      )
    urllib2.urlopen(req).read()
    url2 = "http://u.alimama.com/union/newreport/taobaokeDetail.htm?toPage=1&perPageSize=20&startTime=%s&endTime=%s&DownloadID=&payStatus=&total=0&queryType=1" % (start, end)
    ret = urllib2.urlopen(url2).read().decode('gbk')
    if login_name in ret:
        #=======================================================================
        # 下载excel文件
        #=======================================================================
        try:
            dest_name = download_excel(postdata, headers, user_id, start, end, download_url)
            return dest_name
        except:
            error_msg = "DOWNLOAD EXCEL ERROR %s" %(user_id)
            send_mail('email@address',error_msg,error_msg)
            return False                        
    else:
        return False


#===============================================================================
# 淘宝客抓取
#===============================================================================
def main():
    start,end = getOneMonthAgo()
    yesterday = getYesterDay()
    conn = MySQLdb.connect(host= MYSQL_HOST, user=MYSQL_USER , passwd=MYSQL_PASSWD, db=MYSQL_DB, port=MYSQL_PORT)
    conn.set_character_set('utf8')
    cursor = conn.cursor()
    cursor.execute("set names utf8")
                
    delete_tbk_detail(conn, 'tbk_detail', start, end)
             
    for user in login_info:
        logname = user['logname']
        originalLogpasswd = user['originalLogpasswd']
        login_name=user['login_name']
        user_id = user['user_id']
        
              
        dest_file_name_yesterday = spider_tbk_list(yesterday, yesterday, logname, originalLogpasswd, login_name,user_id)
        if dest_file_name_yesterday!=False:
            tbk_excel(conn, dest_file_name_yesterday, user_id, "tbk_detail_history")
        else:
            send_mail('email@address','TAOBAOKE EXCEL ERROR','TAOBAOKE HISTORY ERROR')
        os.remove(dest_file_name_yesterday)
        
        time.sleep(2)
        
if __name__=="__main__":
    logging = init_logging()
    main()
