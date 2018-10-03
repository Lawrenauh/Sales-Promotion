# -*- coding: utf-8 -*-

from tpdsys import *

adm = admaster()
ado = admonitor()
tvo = tvmonitor()
ucl = uniclick()

ott_pid = ['59','95','176','219','242','263','284','305','323','349','384','1030','1032','1052','1068','1083','1087']

colnames_en = ['agentName', 'date', 'pid', 'tagid', 'imp', 'uimp', 'clk', 'uclk', 'mimp', 'mclk']
colnames_ch = ['日期', '项目ID', '广告位ID', '代理曝光', '代理点击', '媒体曝光', '媒体点击']

df_all = pd.concat([adm.df, ado.df, tvo.df, ucl.df], ignore_index = True).replace('\xa0|,','', regex=True)
df_all['mimp'] = np.nan
df_all['mclk'] = np.nan

output = df_all[df_all.pid.notnull()][colnames_en]
output.loc[output.pid.isin(ott_pid), 'clk'] = output.loc[output.pid.isin(ott_pid), 'uimp']
output.drop(['agentName', 'uimp', 'uclk'], axis=1, inplace=True)
output.columns = colnames_ch
output.replace('^(-|_| )*$','', regex=True, inplace=True)

output.to_csv(r'C:\Users\huhuan3\Desktop\基础数据\三方数据.csv', encoding='gbk', index=False)
# df_all.to_csv(pwd + '\\三方数据 - 原始.csv', encoding='gbk', index=False)

print('running time: %.2f s' % (time.time() - start))
input('\npress any key to exit...')
