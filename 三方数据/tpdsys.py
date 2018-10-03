# -*- coding: utf-8 -*-

import requests, re, json, os, time, datetime
import numpy as np
import pandas as pd
from pandas import DataFrame
from PIL import Image

k = int(input('How many days do you want to update? (Integer): '))

pwd = os.getcwd()

start = time.time()
today = datetime.date.today()
td = datetime.timedelta(days=k)
od = datetime.timedelta(days=1)

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'}

account_list = {
    'ad': [
            {'Agent': '派瑞', 'id': '6', 'email': 'jiangfengjiao@aspiration-cn.com', 'password': 'PRWXjd2018'},
            {'Agent': '美通', 'id': '542', 'email': 'wangyuyao@mtad.cn', 'password': 'Mtad123456'},
            {'Agent': '蓝标', 'id': '604', 'email': 'wenjia.lin@bluefocus.com', 'password': 'Bluefocus2018'},
            {'Agent': '三人行', 'id': '605', 'email': 'songjinlan@topsrx.com', 'password': '!SONG115656'},
            {'Agent': '直签', 'id': '627', 'email': 'zhuzhongqi@jd.com', 'password': 'zhu420012903!!'}
    ],
    'mz': [
            {'Agent': '180', 'username': '180_2017YBL', 'password': 'Dl80JCMZ'},
            {'Agent': '直签', 'username': 'charmgroup_jd', 'password': 'e3fGIFndhh62hB'}
    ],
    'uc': [
            {'Agent': '华扬', 'username': 'jingdong', 'password': '123456'}
    ]
}

outputcolnames = ['agentName', 'date', 'campaignName', 'mediaName', 'channelName', 'placementName', 'campaignId', 'placementId', 'pid', 'tagid', 'imp', 'uimp', 'clk', 'uclk']

class admaster(object):

    def __init__(self):
        pass

    data_1 = {"client_id": "964efdda0ee2d8c74c50", "response_type": "code", "redirect_uri": "https://track.admaster.com.cn/#!/session/create"}
    data_2 = {'client_id': '964efdda0ee2d8c74c50'}
    data   = {'client_id': '0c5b3458b95188065309', 'grant_type': 'password'}

    # columns1 = ['campaignId','mediaId','id','channelName','name','targetUrl','estimateImp','estimateClk','spots'] = [项目id, 媒体id, 广告位id, 频道名称, 广告位名称, 目标链接, 单位预估曝光, 单位预估点击, 排期]
    columns1 = ['campaignId','mediaId','id','channelName','name','targetUrl']
    # columns2 = ['date','media_name','placement_channelName','placement_name','media','placement','imp','uimp','clk','uclk'] = [日期, 媒体名称, 频道名称, 广告位名称, 媒体id, 广告位id, 曝光, 独立曝光, 点击, 独立点击]
    columns2 = ['date','media_name','media','placement','imp','uimp','clk','uclk']
    df1 = DataFrame(columns = columns1)
    df2 = DataFrame(columns = columns2)
    map = {'media_name':'mediaName','name':'placementName','media':'mediaId','placement':'placementId'}

    for l in range(len(account_list['ad'])):

        agent_name = account_list['ad'][l]['Agent']
        agent_id = account_list['ad'][l]['id']
        data['email'] = account_list['ad'][l]['email']
        data['password'] = account_list['ad'][l]['password']

        s = requests.session()

        login_0 = s.post('https://account.admaster.com.cn/api/oauth/access_token', data=data)

        headers['Authorization'] = 'token ' + json.loads(login_0.text)['access_token']
        login_1 = s.post('https://account.admaster.com.cn/api/oauth/authorize', data=data_1, headers=headers)

        data_2['code'] = json.loads(login_1.text)['code']
        login_2 = s.post('https://track.admaster.com.cn/api_v2/token', data=data_2)

        headers['X-Auth-Token'] = json.loads(login_2.text)['access_token']
        login = s.post('https://account.admaster.com.cn/api/oauth/access_token', data=data, headers=headers)

        response = s.get('https://track.admaster.com.cn/api_v2/networks/' + agent_id + '/campaigns?maxResults=5000&includes=brand&sort=-createdAt&startIndex=0', headers=headers)
        program_page = response.json()

        for i in range(len(program_page)):

            ad_pid = program_page[i]['id']
            ad_pname = program_page[i]['name']
            startdate = datetime.datetime.strptime(program_page[i]['startDate'], '%Y-%m-%d').date()
            enddate = datetime.datetime.strptime(program_page[i]['endDate'], '%Y-%m-%d').date()

            if startdate < today <= enddate + td:   # 遍历 前td天到昨天期间 结束的所有项目
                response = s.get('https://track.admaster.com.cn/api_v2/campaigns/' + str(ad_pid) + '/placements?sort=orderNum&includes=material&maxResults=5000', headers=headers)
                if response.json():
                    d1 = DataFrame(response.json())[columns1]
                    d1['campaignName'] = ad_pname
                    d1['agentName'] = agent_name
                    df1 = pd.concat([df1, d1], ignore_index=True)

                response_data = s.get('https://track.admaster.com.cn/api_v2/campaigns/' + str(ad_pid) + '/reports/basics?startDate=' + str(today - td) + '&endDate=' + str(today - od) + '&dimensions=date%2Cmedia%2Cplacement&maxResults=3000&metrics=imp%2Cclk%2Cuimp%2Cuclk%2CctRate&sort=-imp&translate=expand&startIndex=0&platform=hybird', headers=headers)
                if response_data.json():
                    d2 = DataFrame(response_data.json())[columns2]
                    d2['campaign'] = ad_pid
                    df2 = pd.concat([df2, d2], ignore_index=True)

    df = pd.merge(df2, df1, left_on=['campaign','media','placement'], right_on=['campaignId','mediaId','id'], how='left')
    df = df.join(df.channelName.str.extract('[(（ ]*(?P<pid_channel>\d+)\s*-\s*(?P<tagid_channel>\d+)[)） ]*$', expand=True)).rename(columns=map)
    df = df.join(df.targetUrl.str.extract('pid=(?P<pid_url>\d*)&tagid=(?P<tagid_url>\d*)', expand=True))
    df_1 = df[['pid_channel','tagid_channel']]
    df_2 = df[['pid_url','tagid_url']]
    df_1.columns = df_2.columns = ['pid','tagid']
    df = df.join(df_2.combine_first(df_1))
    df = df[outputcolnames]


class admonitor(object):

    def __init__(self):
        pass

    df = DataFrame(columns=['attribution', 'campaignId', 'campaign', 'channel', 'click', 'clickComplete', 'clicker', 'clientCaid', 'clientSpid', 'ctr', 'customize', 'estClick', 'estImp', 'format', 'frequency', 'imp', 'impComplete', 'media', 'page', 'panel', 'platform', 'position', 'saleType', 'showSearchWords', 'size', 'spid', 'type', 'uv'])

    for l in range(len(account_list['mz'])):

        s = requests.session()

        agent = account_list['mz'][l]['Agent']
        username = account_list['mz'][l]['username']
        password = account_list['mz'][l]['password']
        login_0 = s.get('https://admonitor.miaozhen.com/login', headers=headers)
        csrf = re.findall(r'name="_csrf" value="(.*)"', login_0.text)[0]
        data = {
            'username': username,
            'password': password,
            '_csrf': csrf,
            'locale': 'zh_CN'
        }

        login = s.post('https://admonitor.miaozhen.com/userLogin', data=data, headers=headers)
        response = s.post('https://admonitor.miaozhen.com/campaign/data/all?pageSize=1000&pageNum=1&sortBy=&asc=false&searchKey=campaignId&regionId=local',headers=headers)

        campaigns_list = json.loads(response.text)['data']

        data = {
                    'pageSize': '1000',
                    'pageNum': '1',
                    'asc': 'false',
                    'platform': 'pm',
                    'region': '000000000000000000000000',
                    'mediaId': '-1',
                    'target': 'all',
                       }

        for i in range(len(campaigns_list)):

            campaign_info = campaigns_list[i]
            mz_pid = campaign_info['campaignId']
            mz_pname = campaign_info['campaignName']
            start_date = datetime.datetime.strptime(campaign_info['startTime'], '%Y-%m-%d').date()
            end_date = datetime.datetime.strptime(campaign_info['endTime'], '%Y-%m-%d').date()

            if start_date <= today <= end_date + td:
                for j in range(1, k + 1):
                    dat = today - datetime.timedelta(days=j)
                    # expand data items
                    data['campaignId'] = str(mz_pid)
                    data['startDate'] = str(dat)
                    data['endDate'] = str(dat)

                    response = s.post('https://admonitor.miaozhen.com/campaign/data/basic/spots', data=data, headers=headers)
                    tag_page = DataFrame(json.loads(response.text)['data'])
                    tag_page['campaignId'] = mz_pid
                    tag_page['campaign'] = mz_pname
                    tag_page['date'] = dat
                    tag_page['agent'] = agent

                    df = pd.concat([df, tag_page], ignore_index=True)

    df = df.join(df.channel.str.extract('[(（ ]*(?P<pid>\d+)\s*-\s*(?P<tagid>\d+)[)） ]*$', expand=True))
    df = df[['agent', 'date', 'campaign', 'media', 'channel', 'position', 'campaignId', 'spid', 'pid', 'tagid', 'imp', 'uv', 'click', 'clicker']]
    df.columns = outputcolnames


class tvmonitor(object):

    def __init__(self):
        pass

    df = DataFrame(columns=['adSize', 'adType', 'channel', 'click', 'clicker', 'dimensionId', 'fileType', 'imp', 'media', 'position', 'show', 'spid', 'spidStr', 'uv', 'uv_items', 'agent', 'date', 'campaign', 'campaignId'])

    for l in range(len(account_list['mz'])):

        s = requests.session()

        agent = account_list['mz'][l]['Agent']
        username = account_list['mz'][l]['username']
        password = account_list['mz'][l]['password']
        login_0 = s.get('https://admonitor.miaozhen.com/login', headers=headers)
        csrf = re.findall(r'name="_csrf" value="(.*)"', login_0.text)[0]
        data = {
            'username': username,
            'password': password,
            '_csrf': csrf,
            'locale': 'zh_CN'
        }

        login = s.post('https://admonitor.miaozhen.com/userLogin', data=data, headers=headers)
        response = s.get('https://admonitor.miaozhen.com/tvMonitor', headers=headers, allow_redirects=False)
        response = s.get(response.headers['Location'], headers=headers, allow_redirects=False)
        response = s.post('http://tvmonitor.miaozhen.com/dtv/dataInsight/campaignList?pageSize=1000&pageNum=1&sortBy=&asc=false&camType=&status=&searchKey=campaignId&searchValue=&regionId=local', headers=headers)

        campaigns_list = response.json()['data']

        data = {
            'pageSize': '1000',
            'pageNum': '1',
            'asc': 'false',
            'region': '1156000000',
            'pubId': '-1',
            'targetId': 'all'
        }

        for i in range(len(campaigns_list)):
            campaign_info = campaigns_list[i]
            mz_pid = campaign_info['campaignId']
            mz_pname = campaign_info['campaignName']
            start_date = datetime.datetime.strptime(campaign_info['startTime'], '%Y-%m-%d').date()
            end_date = datetime.datetime.strptime(campaign_info['endTime'], '%Y-%m-%d').date()

            if start_date <= today <= end_date + td:
                for j in range(1, k + 1):
                    dat = today - datetime.timedelta(days=j)
                    # expand data items
                    data['campaignId'] = str(mz_pid)
                    data['startTime'] = str(dat)
                    data['endTime'] = str(dat)

                    response = s.post('http://tvmonitor.miaozhen.com/dtv/dashboard/data/jSpotBasic', data=data, headers=headers)
                    tag_page = DataFrame(response.json()['data'])
                    tag_page['campaignId'] = mz_pid
                    tag_page['campaign'] = mz_pname
                    tag_page['date'] = dat
                    tag_page['agent'] = agent

                    df = pd.concat([df, tag_page], ignore_index=True)

    df = df.join(df.channel.str.extract('[(（ ]*(?P<pid>\d+)\s*-\s*(?P<tagid>\d+)[)） ]*$', expand=True))
    df = df[['agent', 'date', 'campaign', 'media', 'channel', 'position', 'campaignId', 'spidStr', 'pid', 'tagid', 'imp', 'uv', 'click', 'clicker']].replace(',', '', regex=True)

    df.columns = outputcolnames


class uniclick(object):

    def __init__(self):
        pass

    df = DataFrame(columns=['CTR', 'Click', 'InventoryName', 'InventoryNameEn', 'MediaName', 'OrderItemName', 'OrderItemNameEn', 'OrderName', 'OrderNameEn', 'PV', 'SiteNameEn', 'StatDate', 'UC', 'UV', 'avgClick', 'avgPV'])

    for l in range(len(account_list['uc'])):

        s = requests.session()

        agent = account_list['uc'][l]['Agent']
        username = account_list['uc'][l]['username']
        password = account_list['uc'][l]['password']

        varcode = s.get('http://data.uniclick.cn/public/getCaptcha')
        f = open(pwd + '\code.png', 'wb')
        f.write(varcode.content)
        f.close()

        img = Image.open(pwd + '\code.png')
        img.show()
        code = input('请输入验证码：')
        data = {
            'username':  username,
            'password':  password,
            'randCode':  code
        }

        login = s.post('http://data.uniclick.cn/public/loginCheck', data=data, headers=headers)

        response_order = s.get('http://data.uniclick.cn/public/listOrder?page=1&keyword=&fields=OrderName&orderby=OrderID&timestatus=-1', headers=headers)
        order_list = json.loads(response_order.text)['items']

        s.cookies = requests.utils.add_dict_to_cookiejar(s.cookies, {'cid': '403'})

        for i in range(len(order_list)):
            pid = order_list[i]['OrderID']
            p_name = order_list[i]['OrderName']
            start_date = datetime.datetime.strptime(order_list[i]['StartTime'], '%Y-%m-%d').date()
            end_date = datetime.datetime.strptime(order_list[i]['EndTime'], '%Y-%m-%d').date()

            total_pages = page = 1
            if start_date < today < (end_date + td):
                while page <= total_pages:
                    response_tag = s.get('http://data.uniclick.cn/generalreport/orderitemReport?BreakDown=0&StartDate=' + str(today - td) + '&EndDate=' + str(today - od) + '&OrderID=' + pid + '&MediaID=0&OrderItemID=0&type=2&page=' + str(page) + '&sort=', headers=headers)
                    data = DataFrame(response_tag.json()['result'])
                    data['agent'] = agent
                    data['pid'] = pid
                    total_pages = (int(response_tag.json()['total']) // 15) + 1
                    df = pd.concat([df, data], ignore_index=True)
                    page += 1

    df = df.join(df.InventoryName.str.extract('[(（ ]*(?P<channel_pid>\d+)\s*-\s*(?P<channel_tagid>\d+)[)） ]*$', expand=True))
    df = df.join(df.OrderItemName.str.extract('[(（ ]*(?P<tag_pid>\d+)\s*-\s*(?P<tag_tagid>\d+)[)） ]*$', expand=True))
    jid1 = df[['channel_pid', 'channel_tagid']]
    jid2 = df[['tag_pid', 'tag_tagid']]
    jid1.columns = jid2.columns = ['p_id', 'tag_id']

    df = df.join(jid1.combine_first(jid2))
    df['placementId'] = np.nan

    df = df[['agent', 'StatDate', 'OrderName', 'MediaName', 'InventoryName', 'OrderItemName', 'pid', 'placementId','p_id', 'tag_id', 'PV', 'UV', 'Click', 'UC']]
    df.columns = outputcolnames
