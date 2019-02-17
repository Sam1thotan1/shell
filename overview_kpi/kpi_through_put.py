# -*- coding: utf-8 -*-
"""
Created on Fri Jan 11 16:43:14 2019

@author: jiajuwu
"""

from datetime import datetime
import os
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession

import time
from pyspark.sql.functions import lit

current_time_str=time.strftime('%Y%m%d %H:%m',time.localtime())
year_rep=current_time_str[:4]
                               
sql_text="""select b.service_region,b.service_group,b.product_en,b.repair_distribution, count(b.fin) through_put from (select case 
when a.repair_age <=3 then '<=3' when a.repair_age > 3 and a.repair_age <=5 
then '3-5' when a.repair_age >5 and a.repair_age <=8 then '5-8' 
when a.repair_age >8 then '>8' end as repair_distribution,a.service_region,a.service_group,a.product_en,a.fin 
from (select year(repair_date)-year(first_registration_date) repair_age, service_region,service_group,product_en,fin 
from revr_bmbs_srv_contract_blue.tgt_claim_all_info where year(repair_date) = '"""+year_rep+"""') a) b 
group by b.service_region,b.service_group,b.product_en,b.repair_distribution """
                                                         
df_fin = spark.sql(sql_text)

month_rep=current_time_str[4:6]

#df_fin=df_fin.withColumn('year',lit(year_rep))
#
#df_fin=df_fin.withColumn('month',lit(month_rep))

df_fin=df_fin.withColumn('last_modified_date',lit(current_time_str))

df_fin=df_fin.select(['service_region','service_group','product_en','repair_distribution','through_put','last_modified_date'])

df_fin.registerTempTable('df')

spark.sql("""insert overwrite table revr_bmbs_srv_contract_blue.tgt_kpi_through_put 
partition(year=%s, month=%s) select * from df"""%(year_rep,month_rep))










