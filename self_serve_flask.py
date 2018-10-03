import os
from flask import Flask, render_template, request
import pandas as pd
import numpy as np
import urllib.request
import os.path
import glob
import datetime
import re
import xlsxwriter
import sqlalchemy as a
from sqlalchemy import create_engine
import subprocess

##__author__ = 'ibininja'

app = Flask(__name__)

APP_ROOT = os.path.dirname(os.path.abspath(__file__))

@app.route("/")
def selfserve():
    username = 'charlescummings'
    password = 'CharlesXad'

    #Stores .csv output of each ad group in the following path

    path = 'C:\Python33\Files\\'
    path = 'C:Files\\'

    #Enter IDs to be used here

    redshift = a.create_engine("postgresql://alexw:3nigm@User@redshift.xad.com:5439/dwenigma")
    redshift_connection = redshift.connect()

    start_time = datetime.date.today() - datetime.timedelta(days=1)
    end_time = datetime.date.today()
    second_day = datetime.date.today() - datetime.timedelta(days=2)

    display_title_string = 'Self_Serve_Under_Pacing_Report_ew'+start_time.strftime('%Y-%m-%d')+'.xlsx'
    display_writer = pd.ExcelWriter(display_title_string)

    ad_return_avails_factor=1.0/0.2

    def find_category(radar_string):
      #print(radar_string)
      x = re.search(r'([0-9].+)', str(radar_string))
      #print(x)
      if x :
        rager = re.sub('[0-9].','',x.group(0))
        line = re.sub('.sum_all.hosts','',rager)
        #print(line)
        return(line)
      else:
        return("")

    def replace_blanks(value_string):
      if int(value_string) >0:
        return value_string
      else:
        return 0

    def find_ad_group_id(rti_string):
        x = re.search(r'\d+.?\d*',rti_string)
        if x:
            rager = re.sub(',','',x.group(0))
            return(rager)
        else:
            return("")

    sql1 = """
    select 
    fact.campaign_id,
    fact.adGroup_id as adgroup_id,
    cdim.campaign_name,
    fact.adgroup_name,
    cdim.salesforce_number,
    fact.start_date,
    fact.end_date,
    fact.market,
    fact.adv_bid_rate,
    fact.enable_locaud,
    fact.ctr_threshold,
    bdim.budget as budget,
    fact.product as product,	                                                
    fact.billability_type,
    bdim.total_lifetime_spent as spend_to_date,
    cdim.currency as rev_currency_name,
    DATEDIFF(day, fact.start_date, fact.end_date) as term,
    DATEDIFF(day, fact.start_date, '{current_date}') as term_to_date,
    (CASE WHEN fact.start_date = '{current_date}' THEN '1st Day' ELSE '' END) as first_day_flag,
    (CASE WHEN fact.start_date < '{second_day_date}' THEN 'Over_48_hours_old' ELSE '' END) as Over_48_hours_old,
    (CASE WHEN fact.end_date = '{current_date}' THEN 'Last Day' ELSE '' END) as last_day_flag,
    sum(case when cpds.timestamp = '{current_date}' then cpds.ad_returned else 0 end) as ad_returned,
    sum(case when cpds.timestamp = '{current_date}' then cpds.ad_impression else 0 end) as ad_impression,
    sum(case when cpds.timestamp = '{current_date}' then cpds.pub_imp_bid else 0 end) as pub_imp_bid,
    sum(case when cpds.timestamp = '{current_date}' then cpds.pub_gross_revenue else 0 end) as pub_gross_revenue,
    sum(case when cpds.timestamp = '{current_date}' then cpds.adv_imp_bid else 0 end) as adv_imp_bid,
    max(gdim.radius) as radius

    FROM adgroup_dimension fact
    LEFT JOIN campaign_dimension cdim ON fact.campaign_id = cdim.campaign_id
    join budget_dimension bdim ON (fact.adgroup_id = bdim.adgroup_id and bdim.entity_type = 1)
    join campaign_daily_summary cpds ON cpds.adgroup_id = fact.adgroup_id
    join geotarget_dimension gdim ON fact.adgroup_id = gdim.adgroup_id
    WHERE fact.start_date < '{current_date}'
    AND fact.end_date > '{current_date}'
    AND cdim.account_type = 'SELFSERVICE' 
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
    limit 20
    """.format(current_date=start_time.strftime('%Y-%m-%d')+' 00:00:00', second_day_date=second_day.strftime('%Y-%m-%d')+' 00:00:00')

    # while True:

        # ##with ad_group_ids = pd.read_csv("sample_sql.csv")
            # try:
    ad_group_ids = pd.read_sql(sql1, redshift_connection)
    ad_group_ids['spend_to_date_2'] = ad_group_ids.spend_to_date
    ad_group_ids['budget_to_date'] = 0
    ad_group_ids['budget_to_date'][ad_group_ids.budget>0] = (ad_group_ids.budget/(ad_group_ids.term+1))*(ad_group_ids.term_to_date+1)
    ad_group_ids['lifetime_pacing'] = 0
    ad_group_ids['lifetime_pacing'][ad_group_ids.budget_to_date>0] = ad_group_ids.spend_to_date/ad_group_ids.budget_to_date
    ad_group_ids['pacing_group'] = -1
    ad_group_ids['pacing_group'][(ad_group_ids.budget_to_date>0) & (ad_group_ids.ad_returned>0)] = round(ad_group_ids.lifetime_pacing * 10,0)/10
    ad_group_ids['pacing_group'][(ad_group_ids.budget_to_date>0) & (ad_group_ids.ad_returned == 0)] = -2
    ad_group_ids['remaining_term']=0
    ad_group_ids['remaining_term']=ad_group_ids.term-ad_group_ids.term_to_date
    ad_group_ids['remaining_budget']=0
    ad_group_ids['remaining_budget']=ad_group_ids.budget-ad_group_ids.spend_to_date
    ad_group_ids['daily_required_spend']=0
    ad_group_ids['daily_required_spend'][ad_group_ids.remaining_term>0]=ad_group_ids.remaining_budget/ad_group_ids.remaining_term
    ad_group_ids['daily_required_impressions']=0
    ad_group_ids['daily_required_impressions'][ad_group_ids.adv_bid_rate>0]=ad_group_ids.daily_required_spend/ad_group_ids.adv_bid_rate
    ad_group_ids['daily_required_avails']=0
    ad_group_ids['daily_required_avails']=ad_group_ids.daily_required_impressions*ad_return_avails_factor
    ad_group_ids['publisher_bid']=0
    ad_group_ids['publisher_bid'][ad_group_ids.ad_returned>0]=ad_group_ids.pub_gross_revenue/ad_group_ids.ad_returned
            # except Exception: continue

    ids = ad_group_ids.adgroup_id


    start_date=start_time.strftime("%Y%m%d")
    end_date=end_time.strftime("%Y%m%d")

    print(start_date)
    print(end_date)

    master_radar_db = {'type':['num_index_results','num_returned','num_kpi_estimated_ctr_maxthreshold_filtered','num_bid_floor_filtered','query_num_returned_filtered','num_freq_cap_user_filtered','num_budget_filtered','budget_filtered_adgrpDocId','num_creative_filtered','num_freq_cap_publisher_filtered','exchange_ad_filtered','num_impressions']}
    ob_df = pd.DataFrame(data=master_radar_db)


    for i in ids:

        password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        top_level_url = "https://radar.xad.com/render?from={start}&until={end}&width=1200&target=nise.adgroups.{id}.*.sum_all.hosts&_uniq=0.6909822267480195&title=nise.adgroups.{id}.*.%27&format=csv".format(id=i, start=start_date, end=end_date)

        password_mgr.add_password(None, top_level_url,username, password)
        handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
        opener = urllib.request.build_opener(handler)
        opener.open(top_level_url)
        urllib.request.install_opener(opener)
        urllib.request.urlretrieve(top_level_url, "output.csv")

        try:

            cov = pd.read_csv("output.csv", header = None)

        except Exception: continue

        (i,)

        cov.columns = ["Filter","Timestamp","Number"]
        cov['Date'] = cov['Timestamp'].str[:10]
        cov["ID"] = "{}".format(i)
        cov["type"] = cov.Filter.apply(find_category)
        cov["targe_date"] = 0

        # print(i)

        pivot_table_example=pd.pivot_table(cov, values = ['Number'],index='type', aggfunc=np.sum, margins = False)
        pivot_table_example.reset_index(inplace=True)
        pivot_table_example = pivot_table_example.rename(columns = {'Number':i})

        ob_df=ob_df.merge(pivot_table_example, on=['type'], how='left')

    ob_df_transposed = ob_df.T
    #ob_df_transposed=ob_df_transposed.convert_objects(convert_numeric=True).fillna(0)
    ob_df_transposed=ob_df_transposed.apply(pd.to_numeric,errors='coerce').fillna(0)
    ob_df_transposed.to_csv(path+'Master_radar_{}.csv'.format('m'),encoding = 'utf-8')
    ob_df_transposed = ob_df_transposed.rename(columns = {'type':'adgroup_id'})
    ob_df_transposed.columns = ['num_index_results','num_returned','num_kpi_estimated_ctr_maxthreshold_filtered','num_bid_floor_filtered','query_num_returned_filtered','num_freq_cap_user_filtered','num_budget_filtered','budget_filtered_adgrpDocId','num_creative_filtered','num_freq_cap_publisher_filtered','exchange_ad_filtered','num_impressions']
    ob_df_transposed.reset_index(inplace=True)
    ob_df_transposed = ob_df_transposed.rename(columns = {'index':'adgroup_id'})
    ob_df_transposed = ob_df_transposed.merge(ad_group_ids,on=['adgroup_id'], how='left')
    ob_df_transposed.num_index_results=pd.to_numeric(ob_df_transposed.num_index_results, errors='coerce')
    ob_df_transposed.num_freq_cap_user_filtered=pd.to_numeric(ob_df_transposed.num_freq_cap_user_filtered, errors='coerce')
    ob_df_transposed.num_kpi_estimated_ctr_maxthreshold_filtered=pd.to_numeric(ob_df_transposed.num_kpi_estimated_ctr_maxthreshold_filtered, errors='coerce')
    ob_df_transposed.num_impressions=pd.to_numeric(ob_df_transposed.num_impressions, errors='coerce')
    ob_df_transposed.num_budget_filtered=pd.to_numeric(ob_df_transposed.num_budget_filtered, errors='coerce')
    ob_df_transposed['user_influenced']=ob_df_transposed.num_index_results-ob_df_transposed.num_freq_cap_user_filtered-ob_df_transposed.num_kpi_estimated_ctr_maxthreshold_filtered
    ob_df_transposed['targeting_adequate']='Targeting OK'
    ob_df_transposed['targeting_adequate'][ob_df_transposed.daily_required_avails>ob_df_transposed.num_index_results]='Targeting Deficient'
    ob_df_transposed['user_filters_adequate']='User Filters OK'
    ob_df_transposed['user_filters_adequate'][ob_df_transposed.daily_required_avails>ob_df_transposed.user_influenced]='User Filter Deficient'
    ob_df_transposed['frequency_capped']='no'
    ob_df_transposed['frequency_capped'][ob_df_transposed.num_freq_cap_user_filtered>0]='yes'
    ob_df_transposed['neptune_budget_filtering']='no'
    ob_df_transposed['neptune_budget_filtering'][ob_df_transposed.num_budget_filtered>0]='yes'

    display_output = ob_df_transposed.query('pacing_group != -2 & pacing_group <0.9 & over_48_hours_old=="Over_48_hours_old" & neptune_budget_filtering=="no"')
    delete_columns = ['num_index_results','num_returned','num_kpi_estimated_ctr_maxthreshold_filtered','num_bid_floor_filtered','query_num_returned_filtered','num_freq_cap_user_filtered','num_budget_filtered','budget_filtered_adgrpDocId','num_creative_filtered','num_freq_cap_publisher_filtered','exchange_ad_filtered','pub_imp_bid','pub_gross_revenue','spend_to_date_2','budget_to_date','lifetime_pacing','daily_required_spend','daily_required_impressions','daily_required_avails','user_influenced','user_filters_adequate','publisher_bid','ad_returned','ad_impression','adv_imp_bid','billability_type','neptune_budget_filtering']
    display_output.drop(delete_columns,1,inplace=True)
    display_output.to_excel(display_writer,'under_pacers')

    redshift_connection.close()


    display_writer.save()
    return 'Done'

if __name__ == "__main__":
    app.run(port=4555, debug=True)