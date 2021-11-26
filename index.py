#encoding:utf-8
import requests
import json
import csv
import time
import datetime
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def getinfos(page,timetext,codes):
    """
    爬虫核心代码
    :param page: 页码
    :param timetext: 时间
    :return:
    """

    headers = { # 伪装请求头
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Cookie':'yfx_c_g_u_id_10000042=_ck21111014045916902108131381807; VISITED_MENU=%5B%2212906%22%2C%228493%22%2C%228491%22%5D; yfx_f_l_v_t_10000042=f_t_1636524299693__r_t_1636524299693__v_t_1636529554868__r_c_0',

        'Host': 'query.sse.com.cn',
        'Referer': 'http://www.sse.com.cn/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36',
    }
    url = "http://query.sse.com.cn/commonQuery.do?"
    par= {
        # 'jsonCallBack': 'jsonpCallback43006',
        'isPagination': 'true',
        'pageHelp.pageSize': '25',
        'pageHelp.pageNo': page, # 页码
        'pageHelp.cacheSize': '1',
        'sqlId': 'COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L',
        #COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L
        'STAT_DATE': timetext, #时间
        'pageHelp.beginPage': page, # 页码
        'pageHelp.endPage': '31',
        '_': '1636527424872',

    }
    requ = requests.get(url,headers=headers,params=par)
    jsondata = requ.text.replace('jsonpCallback25300(','').replace(')','')
    jsondata = json.loads(jsondata)['result']
    for jd in jsondata:

        

        """ 以下默认爬取全部 """
        if 'all' in codes:
            STAT_DATE = jd['STAT_DATE']
            ETF_TYPE = jd['ETF_TYPE']
            SEC_CODE = jd['SEC_CODE']
            NUM = jd['NUM']
            SEC_NAME = jd['SEC_NAME']
            TOT_VOL = jd['TOT_VOL']
            data = [STAT_DATE,ETF_TYPE,SEC_CODE,NUM,SEC_NAME,TOT_VOL]
            saveCsv('上海证券交易所', data) # 保存并写入CSV文件
        
        else:
            """ 以下单独爬取某个基金代码 """
        # code = "515790" #单独爬取某一个基金代码，替换代码即可
            SEC_CODE = jd['SEC_CODE']
            if  SEC_CODE in codes:
                STAT_DATE = jd['STAT_DATE']
                ETF_TYPE = jd['ETF_TYPE']
                SEC_CODE = jd['SEC_CODE']
                NUM = jd['NUM']
                SEC_NAME = jd['SEC_NAME']
                TOT_VOL = jd['TOT_VOL']
                data = [STAT_DATE, ETF_TYPE, SEC_CODE, NUM, SEC_NAME, TOT_VOL]
                saveCsv('上海证券交易所', data)

def runx(codes, start, end):
    """
    爬虫程序运行
    :return:
    """
    col = ['日期', '类型', '基金代码', '数', '基金扩位简称', '总份额（万份）'] # 写入表头
    saveCsv('上海证券交易所', col)


    # t = getEveryDay('2021-05-01', '2021-11-16')# 修改日期，比如我要 1月到9月就修改成 2021-01-01 一定要0的双位数，不然无法识别
    t = getEveryDay(start, end)
    #'2021-07-01' 是开始时间
    # '2021-11-10'是结束时间

    for x in t:
        for p in range(1,16):
            getinfos(p,x,codes)


def saveCsv(filename,content):
    """
    保存为CSV 文件
    :param filename: 文件名
    :param content: 数据内容- 以 list 数据类型
    :return:
    """
    f = open(f'{filename}.csv', 'a+', encoding='utf-8',newline="")
    csv_writer = csv.writer(f)
    csv_writer.writerow(content)
    f.close()
    print(content,"Write to successful")





def getEveryDay(begin_date,end_date):
    # 前闭后闭
    date_list = []
    # begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    # end_date = datetime.datetime.strptime(end_date,"%Y-%m-%d")
    while begin_date <= end_date:
        date_str = begin_date.strftime("%Y-%m-%d")
        date_list.append(date_str)
        begin_date += datetime.timedelta(days=1)
    return date_list

##########################################################

def get_week_num(df):
    date = df['日期']
    todate = datetime.datetime.strptime(date, "%Y-%m-%d")
    return todate.isocalendar()[1]

def get_year_num(df):
    date = df['日期']
    todate = datetime.datetime.strptime(date, "%Y-%m-%d")
    return todate.isocalendar()[0]



def exceldata():
    """
    数据处理
    :return:
    """
    readdata = pd.read_csv('上海证券交易所.csv',encoding='utf-8') # 读取CSV 文件
    df = pd.DataFrame(readdata) # 读取并格式化

    df.to_excel('上海证券交易所.xlsx')
    
    readdata = pd.read_excel('上海证券交易所.xlsx') # 读取CSV 文件
    df = pd.DataFrame(readdata) # 读取所需数据并格式化
    df = df[['日期','类型','基金代码','基金扩位简称','总份额（万份）']]
    
    #日期从远到近
    df.sort_values(by=['日期'],ascending=True)
   
    # 按基金代码分组
    group1 = df.groupby('基金代码')
    
    #获取所有基金代码list
    etfticker = list(group1.size().index)
    
    # 数据基金总数
    total_amount = len(etfticker)
    
    #汇总etf趋势表格
    etfweek = pd.DataFrame(columns = ['基金代码','类型','基金扩位简称','当周平均加仓量','截止日份额'])
    etfweek['基金代码']=etfticker
    
    
    for i in range(0, total_amount):
        # 获取单个ETF代码 
        etf = etfticker[i]
        
        # 获取单个ETF数据
        etfdata = df[df['基金代码']== etf]
        etfdata.index = range(1,len(etfdata)+1)
        
        #获取ETF名称
        etfname = ''
        if len(etfdata)==1: #第一天上市ETF
            etfname=etfdata['基金扩位简称'][1]
        else:
            etfname=etfdata['基金扩位简称'][2]
        #获取ETF类型
        etftype = etfdata['类型'].iloc[0]
        
        #数据开始时间
        startdate = etfdata['日期'].iloc[0]
        
        #数据结束时间
        enddate = etfdata['日期'].iloc[-1]
        
        
        #获取每条数据对应周数与年份
        x = etfdata.copy()
        x.loc[:,"week"]=x.apply(get_week_num,axis=1)
        x.loc[:,"year"]=x.apply(get_year_num,axis=1)
        etfdata = x
        
        #按周和年份进行分组并取平均
        group2 = etfdata.groupby(['week','year'])
        avgdata = pd.DataFrame(group2.mean()['总份额（万份）'])
        avgdata_index = list(group2.size().index)
        
        #以周平均份额建立dataframe
        weeklydata = pd.DataFrame(columns = ['时间','类型','基金代码','基金扩位简称','周平均总份额']) 
        weeklydata['周平均总份额']=np.array(avgdata['总份额（万份）'])
        weeklydata['类型'] = etftype
        weeklydata['基金代码']= etf
        weeklydata['基金扩位简称'] = etfname
        
        for i in range(0,len(weeklydata)):
        
        #     weeklydata['时间'].iloc[i] = 
            # weeklydata.copy()
            weeklydata.loc[i,'时间']= str(np.array(avgdata_index)[i,1])+'-'+str(np.array(avgdata_index)[i,0])
        
        #当周平均加仓量
        if len(weeklydata) == 1:
            etfadd=0
        else:
            etfadd=weeklydata['周平均总份额'].iloc[-1] - weeklydata['周平均总份额'].iloc[-2]
            
            
        #单个ETF数据读入汇总ETF表格
        etfweek.loc[(etfweek['基金代码']==etf),['类型','基金扩位简称','当周平均加仓量','截止日份额']]=\
        [etftype,etfname,etfadd,etfdata['总份额（万份）'].iloc[-1]]
        
        
        
        
        
        
        ######-------------生成趋势图
        plt.rcParams['font.sans-serif']=['simhei'] #图像中文字体调整
        x=weeklydata['时间']
        y=weeklydata['周平均总份额']
        
        
        
        
        
        #标签格式
        frontdict = {'color': 'black',
            'weight': 'bold',
            'size': 18}
        
        
        # finaldata.plot('时间','周平均总份额', color='b',label= '份额（万）',stacked=True) #图片绘制
        plt.figure(figsize=(20,10))
        plt.plot(x,y, color='b',label= '份额（万）')
        #x 轴标签
        plt.xlabel('时间'+' ('+startdate+'--'+enddate+') ',frontdict)
        #y 轴标签
        plt.ylabel('总份额（万份）',frontdict)
        plt.title(str(etf)+'  '+etfname,frontdict)
        plt.grid(True)
        plt.legend(loc='best')
        # plt.text(x[0],35000, 'sdsd',fontsize=12,color = "r")
        
        #周平均份额标注
        for a,b in zip(x,y):
            plt.text(a,b,int(b),fontsize=15,va='center')
        
        #保存图片
        plt.savefig('./'+str(etf)+'  '+etfname+'.jpg',dpi=500)
        print("save picture successfully")
        
    # 保存汇总表格到excel
    etfweek.to_excel('ETF周份额趋势.xlsx')
    
    # group = df.groupby('基金扩位简称')
    # etfnames = list(group.size().index)
    # #df.to_excel('上海证券交易所.xlsx') # 保存问 xlsx 表格文件
    
    # for i in range(len(etfnames)):
    #     etf = etfnames[i]
    #     etfdata = df[df['基金扩位简称']== etf]
    #     plt.rcParams['font.sans-serif']=['simhei']
    #     etfdata.plot('日期','总份额(万份)', color='b',label= '份额（万）')
    #     plt.xlabel('日期')
    #     plt.ylabel('总份额（万份）')
    #     plt.title(str(etf))
    #     plt.grid(True)
    #     plt.legend(loc='best')
    #     plt.savefig('./'+etf+'.jpg',dpi=500)
    
    

        

    

if __name__ == '__main__':
    
    while True:
        try:
            start = datetime.datetime.strptime(input('请输入开始时间(xxxx-xx-xx)：  '), "%Y-%m-%d")
            end = datetime.datetime.strptime(input('请输入截止时间(xxxx-xx-xx)：  '), "%Y-%m-%d")
            break
        except ValueError:
            print("输入的日期格式有误，请再次尝试输入！")
    
    
    
    while True:
        alldata = input('是否查询全部:  ')
        if alldata =='是':
            runx(['all'],start,end)
            break
        elif alldata == '否':
            while True:
                codes=input('请输入代码：  ')
                codelist = codes.split( )
                codevali = [len(c) == 6 and c.isdigit() for c in codelist]
                if False not in codevali:
                    runx(codelist,start,end)
                    break
                else:
                    print('输入代码格式有误，请重新输入')
            break
        else:
            print("输入有误 请输入是或否")
    
    
        
    
    # runx(codes)
     # 爬虫程序
    exceldata() #表格处理
    