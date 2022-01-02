
import pandas as pd
from pytrends.request import TrendReq
#https://pypi.org/project/pytrends/#interest-by-region
import pandas as pd

import pyarrow as pa
import pyarrow.parquet as pq
import datetime as dt

today_dt = dt.datetime.today().strftime("%Y%m%d")

excel_file = '\\mapping_id_cg_cmc.xlsx'
path_in = r'C:\Users\myFolder'
path_out_park = r'C:\park_data\google_trends_daily'
out_file_ts = '\\google_trends_{}.parquet'.format(today_dt)
file_regional = '\\google_trends_country_{}.parquet'.format(today_dt)

out_fileTS_today = r'C:\google_trends_last.parquet'
out_fileRegional_today = r'C:\google_trendsCountry_last.parquet'


out_file_regional = path_out_park + file_regional
out_file_timeSeries = path_out_park + out_file_ts

excel_file_in = path_in + excel_file
df_terms = pd.read_excel(excel_file_in, sheet_name='topic_data')
py_trend_kwList = list(df_terms['google_trends_term'].values)


geo_list = ['US', 'CA', 'DE']#,'GB-ENG', 'FR', 'SG', 'HK', 'KR','JP', None]  # 'GB-ENG'
timeFrames = ['today 5-y', 'today 3-m', 'now 7-d']

df_Big_data = pd.DataFrame()
df_Big_regional_data = pd.DataFrame()
for tf in timeFrames:
    for geo in geo_list:
        #if geo !="US" and tf =='now 7-d':
        #    next
        for kw_used in py_trend_kwList:
            print(kw_used + " - "+ tf)
            kw_used_api = [kw_used]
            pytrends = TrendReq(hl='en-US', tz=360)
            pytrends.build_payload(kw_used_api, cat=0, timeframe=tf, geo=geo, gprop='')  # cat=
            data = pytrends.interest_over_time()

            data = data.drop(labels=['isPartial'], axis='columns')
            data['key_word'] = kw_used
            data['tf'] = tf
            data['geo'] = geo
            data['date'] = data.index
            data.rename(columns={data.columns[0]: "score"}, inplace=True)
            region_data = pytrends.interest_by_region(resolution='COUNTRY', inc_low_vol=True, inc_geo_code=False)
            region_data['kw'] = kw_used
            region_data['date'] = today_dt
            df_Big_regional_data = df_Big_regional_data.append(region_data, ignore_index=True)


            df_Big_data = df_Big_data.append(data, ignore_index=True)



df_Big_data.to_parquet(out_file_timeSeries, engine ='pyarrow')
df_Big_data.to_parquet(out_fileTS_today, engine ='pyarrow')

df_Big_regional_data.to_parquet(out_file_regional, engine='pyarrow')
df_Big_regional_data.to_parquet(out_fileRegional_today, engine='pyarrow')
