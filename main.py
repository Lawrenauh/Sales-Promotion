# -*- coding: utf-8 -*-

import os, sys, re, glob, datetime, time, openpyxl
import numpy as np
import pandas as pd

start0 = time.time()
today = datetime.date.today()
pwdir = os.getcwd()
userdir = os.environ['HOMEPATH']
pid = []

path_list_1 = glob.glob(userdir + '\Downloads\展示促销常规数据推送*天_' + today.strftime('%Y%m%d') + '*.csv')

# 自动匹配更新天数 k
k = int(re.findall(r'推送(\d*)天', path_list_1[0])[0])
fd = today - datetime.timedelta(days=k)
td = today - datetime.timedelta(days=1)
print('\n程序已启动，即将更新 %d 天数据!\n' % k)
time.sleep(1)

print('读取预备文件...')
start1 = time.time()
data_all = pd.read_csv(pwdir + '\基础数据\data_all.csv', encoding='gbk', parse_dates = ['日期', '统计日期'], low_memory = False)
merge_header = pd.read_excel(pwdir + '\Shared\表头.xlsx', sheet_name = '导出表头').columns
mapping = pd.read_excel(pwdir + '\Shared\表头.xlsx', sheet_name = '对照', index_col = 0).to_dict()['导出']
data_tz = pd.read_csv(pwdir + '\点位调整\点位调整.csv', encoding='gbk', parse_dates = ['日期'], low_memory = False)

# 读取天宫系统导出数据/集市推送数据
df1_0 = pd.read_csv(path_list_1[0], encoding = 'gbk', parse_dates = [0])

# 读取历史梳理表df2_0 和系统导出梳理表df2_1, 并合并成全量梳理表df2
df2_0 = pd.read_csv(pwdir + '\梳理表\历史梳理表.csv', encoding='gbk', parse_dates = ['日期'], low_memory = False)
df2_1 = pd.read_csv(pwdir + '\梳理表\梳理表导出.csv', encoding='gbk', parse_dates = ['日期'], low_memory = False)
df2 = pd.concat([df2_0, df2_1], ignore_index = True)
df2['设备类型'] = df2['媒体类型'].apply(lambda x: 'M' if x[:3]=='无线端' else 'PC')
df2['日期'] = pd.to_datetime(df2['日期'], errors = 'coerce')
print('文件加载完毕! 耗时: %.2f s\n' % (time.time()-start1))
time.sleep(1)

# 梳理ID: 1.不可加监测点位及其第二天的余量数据; 2.历史遗漏点位
print('正在梳理新增的ID...')
start2 = time.time()
df1_plus1 = data_all[data_all.日期 >= '2018-01-01'][['日期','项目ID','广告位ID']]
df1_plus1['辅助'] = 1
df1_plus2_1 = df2_1[df2_1.日期 < str(today)][['日期','项目ID','广告位ID']]
df1_plus2_2 = df2_1[df2_1.日期 < str(td)][['日期','项目ID','广告位ID']]
df1_plus2_2.日期 = df1_plus2_2.日期 + datetime.timedelta(days=1)
df1_plus2_1['辅助'] = df1_plus2_2['辅助'] = 2

df1_plus = pd.concat([df1_plus1, df1_plus2_1, df1_plus2_2], ignore_index = True)
df1_plus = pd.concat([df1_plus1, df1_plus2_1, df1_plus2_2], ignore_index = True).drop_duplicates(['日期','项目ID','广告位ID'])
df1_plus = df1_plus[df1_plus.辅助 == 2][['日期','项目ID','广告位ID']]
df1_plus.columns = ['日期','项目id','资源位id']
df1_plus = df1_plus.reindex(columns = df1_0.columns)

# 数据推送表ID 与 梳理表ID 合并
df1 = pd.concat([df1_0,df1_plus], ignore_index = True).drop_duplicates(['日期','项目id','资源位id']).fillna(0)
print('新增ID梳理完毕! 耗时：%.2f s\n' % (time.time()-start2))
time.sleep(1)

# 合并后的ID 与全量梳理表关联
print('新增ID正在与梳理表关联匹配...')
start3 = time.time()
df3 = pd.merge(df1, df2.drop_duplicates(['项目ID', '广告位ID']), left_on = ['项目id','资源位id'], right_on = ['项目ID','广告位ID'], how = 'left')
df3[['排期费用', '实际费用']] = 0
df3['资源属性'] = '余量'
merge = pd.merge(df1, df2, left_on = ['日期','项目id','资源位id'], right_on = ['日期','项目ID','广告位ID'], how = 'left').combine_first(df3)
merge.loc[merge.资源属性 == '余量', ['预估曝光', '预估点击']] = 0
merge.rename(columns = mapping, inplace = True)
merge = merge.reindex(columns = list(merge.columns) + ['代理曝光', '代理点击', '媒体曝光', '媒体点击'], fill_value = 0)
output = merge[merge_header][1:]
output[pd.notnull(output.项目ID)].to_csv(pwdir + '\Output\merge_' + today.strftime('%Y%m%d') + '.csv', encoding='gbk', index=False)
print('匹配完毕，merge_today 生成! 耗时: %.2f s\n' % (time.time()-start3))
time.sleep(1)

# 新增基础数据合并进全量数据；如有重复，是否用新数据替换
print('正在校验与合并 data_today 和 data_all...')
start4 = time.time()
date_max = data_all.日期.max()
date_min = df1_0.日期.min()
diff = (date_min - date_max).days
duplicate = list(np.intersect1d(data_all.日期, df1_0.日期))
while True:
    if diff < 1:
        print("Cautions 1: there are duplicates in date column between 'data_all'(%s) and (%s)'merge_today'!" % (date_max.strftime('%Y-%m-%d'), date_min.strftime('%Y-%m-%d')))
        try:
            rep = int(input("Do you want to replace it? (No: 0 or Yes: 1 or Cancel:2) "))
            if rep == 0:
                df1.drop(df1[df1.日期.isin(duplicate)].index, axis=0, inplace=True)
                break
            elif rep == 1:
                data_all.drop(data_all[data_all.日期.isin(duplicate)].index, axis=0, inplace=True)
                break
            elif rep == 2:
                print('program is about to exit...')
                time.sleep(2)
                sys.exit()
            else:
                print('Only 0(no) or 1(yes) or 2(cancel) is accepted, please re-Enter...')
        except Exception as e:
            print('Please input Integer!')
    elif diff > 1:
        print("Cautions 2: there are INCONTINUOUS dates between 'data_all'(%s) and (%s)'merge_today'!" % (date_max.strftime('%Y-%m-%d'), date_min.strftime('%Y-%m-%d')))
        try:
            inp = int(input("Do you want to continue...? (Yes: 1 or Cancel:2) "))
            if inp == 1:
                break
            elif inp == 2:
                print('program is about to exit...')
                time.sleep(2)
                sys.exit()
            else:
                print('Only 1(yes) or 2(cancel) is accepted, please Re-Enter...')
        except Exception as e:
            print('Please input Integer!')
    else:
        break

data_all = pd.concat([data_all, df1], ignore_index=True).reindex(columns = data_all.columns)
print('合并完毕! 耗时 %.2f s\n' % (time.time()-start4))

# 费用调整
print('费用调整和预估数据...')
start5 = time.time()
data_all.loc[data_all.项目ID >= 1000,['排期费用','实际费用','预估曝光','预估点击']] = 0
data_tz0 = df2_1[['日期','项目ID','广告位ID','排期费用','实际费用','预估曝光','预估点击']]
data_tz = pd.concat([data_tz, data_tz0],ignore_index=True).drop_duplicates(['日期','项目ID','广告位ID'])
data_all = pd.merge(data_all, data_tz, on=['日期','项目ID','广告位ID'], how='left', suffixes=('','_'))
index = data_all.index[data_all['排期费用_'].notnull()]
df2 = data_all.loc[index, ['排期费用_','实际费用_','预估曝光_','预估点击_']]
df2.columns = ['排期费用','实际费用','预估曝光','预估点击']
data_all.loc[index, ['排期费用','实际费用','预估曝光','预估点击']] = df2
data_all.drop(['排期费用_','实际费用_','预估曝光_','预估点击_'],axis=1,inplace=True)
print('调整完毕! 耗时: %.2f s\n' % (time.time()-start5))

# 添加第三方数据和媒体数据
input('即将添加三方数据，请先确认三方数据已经更新! press any key to continue...')
start6 = time.time()
data_sf = pd.read_csv(pwdir + '\三方数据\三方数据.csv', encoding='gbk', parse_dates=['日期'], low_memory=False)
data_sf = data_sf.groupby([data_sf['日期'],data_sf['项目ID'],data_sf['广告位ID']]).sum().reset_index().replace(0, np.nan)
data_all = pd.merge(data_all,data_sf,on=['日期','项目ID','广告位ID'],how='left',suffixes=('','_'))
data_sf0 = data_all[['代理曝光','代理点击','媒体曝光','媒体点击']]
data_sf1 = data_all[['代理曝光_','代理点击_','媒体曝光_','媒体点击_']]
data_sf1.columns = data_sf0.columns
data_all[['代理曝光','代理点击','媒体曝光','媒体点击']]=data_sf1.combine_first(data_sf0)
data_all.drop(['代理曝光_','代理点击_','媒体曝光_','媒体点击_'], axis=1,inplace=True)
print('三方数据更新完毕! 耗时: %.2f s\n' % (time.time()-start6))

# 日报基础数据导出
input('即将导出日报基础数据，请先确认项目概况表已经更新! press any key to continue...')
start7 = time.time()
wb = openpyxl.load_workbook(pwdir + '\展示促销项目概况_v3.1.xlsm', read_only=True)
ws = wb['项目进度']
for r in ws.rows:
    if r[7].value in [1, 1.4, 1.5, 1.9, 2] and not r[6].value:
        pid.append(r[0].value)
wb.close()
report = data_all[data_all['项目ID'].isin(pid)].copy()
report.loc[report.媒体类型.str.contains('无线端-', regex=True), '媒体类型'] = '无线端'
report.to_csv(pwdir + '\Shared\日报基础数据.csv', encoding='gbk', index=False)
print('导出完毕! 耗时: %.2f s\n' % (time.time() - start7))

# 基础数据整理
print('更新基础数据...')
start8 = time.time()
data_all[['预估曝光','预估点击']] = data_all[['预估曝光','预估点击']].replace('-','0',regex=True)
data_all[['排期费用','实际费用','预估曝光','预估点击','广告位ID','项目ID']] = data_all[['排期费用','实际费用','预估曝光','预估点击','广告位ID','项目ID']].astype('float')

data_all.to_csv(pwdir + '\基础数据\data_all_new.csv',encoding='gbk',index=False)

print('基础数据已更新! 耗时: %.2f s\n' % (time.time() - start8))
print('程序运行完毕! 总耗时: %.2f s\n' % (time.time() - start0))
input('press any key to exit...')

