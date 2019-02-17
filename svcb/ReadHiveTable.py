# -*- coding: utf-8 -*-
"""
Created on Fri May 11 11:38:26 2018

@author: jiajuwu and junyi
"""

from datetime import datetime
from pyspark.sql.functions import udf,lit,col
from pyspark.sql.types import *

#import svcb.ETL as etl

  

def fillna_gssn(gssn_old,gssn_new):
    if gssn_new:
        gssn_new_changed = gssn_new
    else:
        gssn_new_changed = gssn_old
    
    return gssn_new_changed


def correct_vehicle_age(vehicle_age):
    if vehicle_age:
        if vehicle_age < 0:
            vehicle_age = 0
        else:
            vehicle_age = vehicle_age        
    else:
        vehicle_age = vehicle_age
    
    return vehicle_age


    
class Contract_Info:
    
    def __init__(self,spark_context,dealer_id,dealer_change):
        self.sc = spark_context
        self.dealer_id = dealer_id
        self.dealer_change=dealer_change
        
        
    def _get_contract_info(self):
        
        """
        Load data
        """ 
        print("Loading contract_info data")

        sql_text_model = """select 
                            a.baumuster,
                            a.class,
                            a.model 
                            from (select substr(fin,1,6) baumuster,
                            regexp_extract(vehicle_model_series,'([^0-9])*',0) as class,
                            sales_designation_of_model model 
                            from datalake_aqua.dw_vhcl_aqua_claimes_overall ) a 
                            group by a.baumuster,a.class,a.model"""
         model_new = self.sc.sql(sql_text_model)
         model_old = self.sc.sql("select * from revr_bmbs_srv_contract_blue.src_model_info) 
         model_info = model_old.union(model_new)
         model_info=model_info.drop_duplicates((['Baumuster_6']))
         model_info.registerTempTable("tmp_model_info")
         self.sc.sql("truncate table revr_bmbs_srv_contract_blue.src_model_info")
         self.sc.sql("insert into table revr_bmbs_srv_contract_blue.src_model_info select * from tmp_model_info") 


        print("Loading contract_info data")    
        #contract_info
        sql_text_contract = """select q.*, 
        cast(q.signature_date as int) - cast(q.first_registration_date as int) vehicle_age 
        from (select s.*, 
        case when substr(s.baumuster_6,1,3) == '453' or substr(s.baumuster_6,1,3) == '451' 
        then 'Smart' else 'Mercedes-Benz' end as brand 
        from 
        (select cn.contract_number,
        cn.external_id,
        cn.source_system,
        cn.contract_state,
        cn.product,
        cn.vega_code,
        cn.fin,
        cn.contract_start,
        cn.first_registration_date,
        cn.planned_contract_end,
        cn.contract_duration,
        cn.start_mileage,
        cn.end_mileage,
        substr(cn.signature_date,1,8) as signature_date,
        cn.activation_date,
        cn.value,
        substr(cn.fin,4,6) as baumuster_6, 
        substr(cn.external_id,5,6) as gssn,
        mi.class,
        mi.model,
        dc.gssn_new 
        from revr_bmbs_srv_contract_blue.src_contract_info cn 
        
        left join revr_bmbs_srv_contract_blue.src_model_info mi 
        on substr(cn.fin,4,6) = mi.baumuster_6 
        
        left join revr_bmbs_srv_contract_blue.src_dealer_change_info dc 
        on substr(cn.external_id,5,6) = dc.gssn_old) s where s.signature_date != '' 
        and s.signature_date is not null and s.vega_code != 'PC_EXCELLE') q 
        
        """


        
        contract_info = self.sc.sql(sql_text_contract)
        
        cols = StructType([StructField('ContractNumber',StringType(),True),
                            StructField('External_ID',StringType(),True),
                            StructField('Source_System',StringType(),True),
                            StructField('Contract_State',StringType(),True),
                            StructField('Product',StringType(),True),
                            StructField('Vega_Code',StringType(),True),
                            StructField('Fin',StringType(),True),
                            StructField('Contract_Start',StringType(),True),
                            StructField('First_Registration_Date',StringType(),True),
                            StructField('Planned_Contract_End',StringType(),True),
                            StructField('Contract_Duration',IntegerType(),True),
                            StructField('Start_Mileage', IntegerType(),True),
                            StructField('End_Mileage', IntegerType(),True),
                            StructField('Signature_Date',StringType(),True),
                            StructField('Activation_Date',StringType(),True),
                            StructField('Value', IntegerType(),True),
                            StructField('Baumuster_6',StringType(),True),
                            StructField('GSSN',StringType(),True),
                            StructField('Class',StringType(),True),
                            StructField('Model',StringType(),True),
                            StructField('GSSN_New',StringType(),True),
                            StructField('Brand', StringType(),True),
                            StructField('Vehicle_Age', IntegerType(),True)])
    
        contract_info = self.sc.createDataFrame(contract_info.rdd,cols)

#        """
#        missing info 
#        """
#        #missing product
#        spark.sql(""" 
#        insert overwrite table revr_bmbs_srv_contract_blue.tgt_miss_product_contract_info
#        select c.* from (select 
#        d.contract_number,
#        d.external_id,
#        d.source_system,
#        d.contract_state,
#        d.product,
#        d.vega_code,
#        d.fin,
#        d.contract_start,
#        d.first_registration_date,
#        d.planned_contract_end,
#        d.contract_duration,
#        d.start_mileage,
#        d.end_mileage,
#        d.signature_date,
#        d.activation_date,
#        d.value,
#        d.baumuster_6
#        from (
#        select a.*,substr(a.fin,4,6) baumuster_6,b.producten from revr_bmbs_srv_contract_blue.src_contract_info a 
#        left join 
#        revr_bmbs_srv_contract_blue.src_product_isp_info b
#        on a.product=b.product and a.vega_code = b.vegacode)d
#        where d.producten is null)c """)
 
#        #missing product code 
#        spark.sql("""
#        insert overwrite table revr_bmbs_srv_contract_blue.tgt_miss_product_vega_code
#        select product,vega_code from revr_bmbs_srv_contract_blue.tgt_miss_product_contract_info a  group by product,vega_code
#        """)

#        #missing baumuster_6 check
#        spark.sql("""insert overwrite table revr_bmbs_srv_contract_blue.tgt_miss_baumuster_contract_info 
#        select e.* from (select 
#        c.contract_number,
#        c.external_id,
#        c.source_system,
#        c.contract_state,
#        c.product,
#        c.vega_code,
#        c.fin,
#        c.contract_start,
#        c.first_registration_date,
#        c.planned_contract_end,
#        c.contract_duration,
#        c.start_mileage,
#        c.end_mileage,
#        c.signature_date,
#        c.activation_date,
#        c.value
#        c.baumuster_6
#        from 
#        (select a.*,b.model from (select cn.*,substr(cn.fin,4,6) as baumuster_6 from revr_bmbs_srv_contract_blue.src_contract_info cn )a 
#        left join 
#        (select baumuster_6,model from revr_bmbs_srv_contract_blue.src_model_info)b
#        on a.baumuster_6=b.baumuster_6)c where c.model is null) e """)
#
#        spark.sql("""insert overwrite table revr_bmbs_srv_contract_blue.tgt_miss_baumuster_code
#        select distinct(a.baumuster_6) baumuster_6 from revr_bmbs_srv_contract_blue.tgt_miss_baumuster_contract_info a""")
#
#        #missing gssn       
#        spark.sql("""
#        insert overwrite table revr_bmbs_srv_contract_blue.tgt_miss_gssn_contract_essence_info
#        select contractnumber
#        ,fin
#        ,baumuster_6
#        ,brand
#        ,class
#        ,model
#        ,first_registration_date
#        ,signature_date
#        ,vehicle_age
#        ,product_en
#        ,product_line
#        ,value
#        ,oil
#        ,cgi
#        ,dccount
#        ,a_count
#        ,b_count
#        ,ac_filter_count
#        ,spark_plug_count
#        ,gssn
#        ,dealer_name_cn
#        ,owner_group 
#        ,region
#        ,province
#        ,city
#        ,servicetimes
#        ,a
#        ,b
#        ,ac
#        ,sp
#        ,detail
#        ,remainservice
#        ,status
#        ,costtillnow
#        ,balance
#        ,value_a
#        ,value_b
#        ,value_sp
#        ,value_ac
#        ,totalupperlimit
#        ,othercost
#        ,gap
#        ,exceedamount
#        ,repairdate
#        ,days_to_next_service_up
#        ,days_to_next_service_low
#        ,next_service_date_up
#        ,next_service_date_low
#        ,year
#        ,cost_2015
#        ,cost_2016
#        ,cost_2017
#        ,cost_2018
#        ,remainbalance_2015
#        ,remainbalance_2016
#        ,remainbalance_2017
#        ,remainbalance_2018
#        from revr_bmbs_srv_contract_blue.tgt_contract_essence_info a where owner_group is null""")
#              
        """
        Data preprocessing
        """
        print("Data preprocessing")
        
        #update GSSN
        contract_info = contract_info.replace(['WLZL10','WGGR23','EHZLI0','NTAR23','NLCD23','NTYR23','NCDI23'], \
                                              ['WLZL20','WGGR10','EHZL10','NTAR10','NLCD10','NTYR10','NCDI10'],'GSSN')

        #convert string to datetime
        strptime_period = udf(lambda x: datetime.strptime(x, '%Y%m%d') if x else x, DateType())
        
        date_columns = {'Signature_Date': strptime_period,
                        'First_Registration_Date': strptime_period,
                        'Contract_Start': strptime_period,
                        'Planned_Contract_End': strptime_period}
        
        for date_column in date_columns:
            contract_info = contract_info.withColumn(date_column, \
                                                     date_columns[date_column](date_column))
            
        contract_info_id=contract_info.filter(contract_info.External_ID.like("%/%"))        
        #contract_info_id = isp_contract.filter((isp_contract.SourceSystem == 'extContractSys')|(isp_contract.SourceSystem == 'migration'))
        contract_info_id = contract_info_id.withColumn('Dealer_ID',contract_info_id.External_ID.substr(1, 3))
        contract_info_id = contract_info_id.drop('GSSN','GSSN_New')
        contract_info_id = contract_info_id.join(self.dealer_id.select(['Dealer_ID','GSSN']), 'Dealer_ID', 'left')

#        """
#        missing dealerid
#        """        
#        miss_dealer=contract_info_id.filter(contract_info_id.GSSN.isNull())
#        miss_dealer.registerTemtable("mis_id")
#        #miss dealer info 
#        spark.sql("""insert overwrite table revr_bmbs_srv_contract_blue.tgt_miss_dealerid 
#        select * from  mis_id""")
#        #miss dealer id
#        spark.sql("""insert overwrite table revr_bmbs_srv_contract_blue.tgt_miss_dealerid_code 
#        select distinct(a.dealer_id) from  tgt_miss_dealerid a""")       
        
        cond=[contract_info_id.GSSN == self.dealer_change.GSSN_Old]
        contract_info_id = contract_info_id.join(self.dealer_change.select(['GSSN_Old','GSSN_New']), cond,'left')
        func_fillna_gssn = udf(fillna_gssn, StringType())        
        contract_info_id = contract_info_id.withColumn('GSSN_New_Changed', func_fillna_gssn('GSSN','GSSN_New'))
        contract_info_id = contract_info_id.drop('GSSN_Old', 'Dealer_ID')
        contract_info_id = contract_info_id.drop('GSSN','GSSN_New')
        contract_info_id = contract_info_id.withColumnRenamed('GSSN_New_Changed','GSSN')

#        contract_info_id = contract_info_id.withColumn('GSSNNew',contract_info_id.GSSN_New)
#        #change CGI and OIL to be null
#        contract_info_id = contract_info_id.withColumn('CGI',lit(''))    
#        contract_info_id = contract_info_id.withColumn('OIL',lit(''))    

        contract_info_gssn=contract_info.filter(~contract_info.External_ID.like("%/%"))
        func_fillna_gssn = udf(fillna_gssn, StringType())        
        contract_info_gssn = contract_info_gssn.withColumn('GSSN_New_Changed', func_fillna_gssn('GSSN','GSSN_New'))
        contract_info_gssn = contract_info_gssn.drop('GSSN','GSSN_New')
        contract_info_gssn = contract_info_gssn.withColumnRenamed('GSSN_New_Changed','GSSN')

        contract_info = contract_info_gssn.union(contract_info_id)

        func_correct_vehicle_age = udf(correct_vehicle_age, IntegerType())        
        contract_info = contract_info.withColumn('Vehicle_Age', func_correct_vehicle_age('Vehicle_Age'))

  
        return contract_info



def repair_age(RepairDate,FirstRegistrationDate):
    RepairAge = RepairDate - FirstRegistrationDate
    return RepairAge.days       

#special situation: repair date = first_registration_date 
#so the repair_age = 0.
#if it is 0, it will cause division by zero      
def mileage_per_day(MileageWhenRepair,RepairAge):
    if RepairAge == 0:
        MileagePerDay = 0
    else:
        MileagePerDay = MileageWhenRepair / RepairAge
    return MileagePerDay
      
        
    
class Cost_Info:
        
    def __init__(self,spark_context):
        self.sc = spark_context
    

    def _get_cost_info(self,firstregday):
        
        """
        Load data
        """ 
        print("Loading contract_info data")   
        
        """
        1. query data
        """
        
        cost_query = \
        """select contractnumber,
        externalid,
        product,
        vegacode,
        workshop,
        claimno, 
        cast(claim_date as string) claim_date, 
        cast(repair_date as string) repair_date, 
        cast(vegacredit_date as string) vegacredit_date,
        damagecode,
        claim_amount,
        mileage_when_repair 
        from revr_bmbs_srv_contract_blue.src_cost_info
        where claim_amount != 0 and vegacode != 'PC_EXCELLE' """
        
        """
        1.1 execute query
        """
        print("Load data to table cost_detail")
        cost =  self.sc.sql(cost_query)


        cols = StructType([StructField('ContractNumber', StringType(),True),
                            StructField('ExternalID', StringType(),True),
                            StructField('Product', StringType(),True),
                            StructField('VegaCode', StringType(),True),
                            StructField('Workshop', IntegerType(),True),
                            StructField('Claimno', StringType(),True),
                            StructField('Claim_Date', StringType(),True),
                            StructField('Repair_Date', StringType(),True),
                            StructField('VegaCredit_Date', StringType(),True),
                            StructField('DamageCode', StringType(),True),
                            StructField('Claim_Amount', FloatType(),True),
                            StructField('Mileage_When_Repair', IntegerType(),True)])
    
        cost = self.sc.createDataFrame(cost.rdd,cols)


        """
        1.2 processing data
        """
        print("Processing data....")
        
        #update Workshop
        cost = cost.replace(['82049'],['82067'],'Workshop')
        
        #convert string to datetime
        strptime_period = udf(lambda x: datetime.strptime(x,'%Y%m%d') if x else x, DateType())        
        
        date_columns = {'VegaCredit_Date': strptime_period
                        , 'Repair_Date': strptime_period
                        , 'Claim_Date': strptime_period}
        
        for date_column in date_columns:
            cost = cost.withColumn(date_column, \
            date_columns[date_column](date_column))
        
        #add FirstRegistrationDate column of contract into cost
        firstregday = firstregday.withColumnRenamed('ContractNumber','Contract_Number')
        cost = cost.join(firstregday, cost.ContractNumber == firstregday.Contract_Number,\
                         how = 'inner')
        
        cost = cost.drop('Contract_Number')

        #generate RepairAge column
        func_repair_age = udf(repair_age,IntegerType())
        cost = cost.withColumn('Repair_Age',func_repair_age('Repair_Date', \
        'First_Registration_Date'))

        #generate MileagePerDay column     
        func_mileage_per_day = udf(mileage_per_day,FloatType())
        cost = cost.withColumn('Mileage_Per_Day',\
                               func_mileage_per_day('Mileage_When_Repair', 'Repair_Age'))
        
        cost = cost.withColumnRenamed('VegaCode','Vega_Code')

        return cost    
    
    
    
    
class DealerMaster:
    
    def __init__(self, spark_context):
        self.sc = spark_context
        
    #create view dealerid_info and drop duplicated    
    def create_dealerid_info(self):


        sql_text_dealer = """select 
        dealer_id,
        workshop,
        gssn,
        sap,
        dealer_name_cn,
        owner_group,
        region,
        province,
        city 
        from revr_bmbs_srv_contract_blue.src_dealer_info"""
        dealerid_info = self.sc.sql(sql_text_dealer)
        
        cols = StructType([StructField('Dealer_ID', StringType(),True),
                            StructField('Workshop', StringType(),True),
                            StructField('GSSN', StringType(),True),
                            StructField('SAP', StringType(),True),
                            StructField('Dealer_Name_CN', StringType(),True),
                            StructField('Owner_Group', StringType(),True),
                            StructField('Region', StringType(),True),
                            StructField('Province', StringType(),True),
                            StructField('City', StringType(),True)])
    
        dealerid_info = self.sc.createDataFrame(dealerid_info.rdd,cols)

    
        return dealerid_info
    
        
    #create view dealer workshop_info and drop duplicated   
    def create_workshop_info(self):
               
        
        sql_text_dealer_workshop = """select  * from revr_bmbs_srv_contract_blue.src_dealer_workshop_info"""
        workshop_info = self.sc.sql(sql_text_dealer_workshop)
 
        
        cols = StructType([StructField('Workshop', IntegerType(),True),
                            StructField('Dealer_Name_Cn', StringType(),True),
                            StructField('Owner_Group', StringType(),True),
                            StructField('Region', StringType(),True),
                            StructField('Province', StringType(),True),
                            StructField('City', StringType(),True)])
    
        workshop_info = self.sc.createDataFrame(workshop_info.rdd,cols)
        
        return workshop_info
    

    #create view gssn_info and drop duplicated
    def create_gssn_info(self):
        
        sql_text_dealer_gssn = """ select  * from revr_bmbs_srv_contract_blue.src_dealer_gssn_info """  
        gssn_info = self.sc.sql(sql_text_dealer_gssn)  

        cols = StructType([StructField('GSSN', StringType(),True),
                            StructField('Dealer_Name_CN', StringType(),True),
                            StructField('Owner_Group', StringType(),True),
                            StructField('Region', StringType(),True),
                            StructField('Province', StringType(),True),
                            StructField('City', StringType(),True)])
    
        gssn_info = self.sc.createDataFrame(gssn_info.rdd,cols)

        
        return gssn_info
          
        

def replace_char(string):
    if string:
        if string.startswith('1931'):
            string = string.replace('1931','1933')
    return string                
        

def replace_char_back(string):
    if string:
        if string.startswith('1933'):
            string = string.replace('1933','1931')
    return string                
        

class Cesar_Info:

    def __init__(self, spark_context):
        self.sc = spark_context
        
    def _get_cesar_info(self, year_str_list, dealer_sap_df):

        for i in range(len(year_str_list)):
            if i==0:
                sql_cesar_inital = """select year,month,model,brand,bm4,\
                bm6,vehicle_id,\
                substring(region,8) region,\
                cast(dealer as string) sap,\
                dealer_cn from revr_bmbs_srv_contract_blue.src_cesar_info_""" 
                    
                sql_cesar_text = sql_cesar_inital + str(year_str_list[i])
                    
            elif i != 0:
                sql_cesar_text = sql_cesar_text + " union all (" + sql_cesar_inital \
                                                               + str(year_str_list[i]) + ")"
                
            else:
                break
        
        sql_cesar_text = """create or replace view 
        revr_bmbs_srv_contract_blue.v_tmp_cesar_info as (""" + sql_cesar_text + ")"  
                                                        
        self.sc.sql(sql_cesar_text)
        cesar_info = self.sc.sql("select * from revr_bmbs_srv_contract_blue.v_tmp_cesar_info")
       
        cols = StructType([StructField('Year', IntegerType(),True),
                            StructField('Month', IntegerType(),True),
                            StructField('Model', StringType(),True),
                            StructField('Brand', StringType(),True),
                            StructField('BM4', IntegerType(),True),
                            StructField('BM6', IntegerType(),True),
                            StructField('Vehicle_ID', StringType(),True),
                            StructField('Region', StringType(),True),
                            StructField('SAP', StringType(),True),
                            StructField('Dealer_CN', StringType(),True)])
    
        cesar_info = self.sc.createDataFrame(cesar_info.rdd,cols)
        
        
        #replace 1931 with 1933 in dealer column        
        func_replace_char = udf(replace_char)
        cesar_info = cesar_info.withColumn('SAP', func_replace_char('SAP'))
        
        """
        calculate montly vehicle sales
        """
        dealersales_monthly = cesar_info.groupby(['SAP','Brand','Year','Month']).agg({'Vehicle_ID': 'count'})
        #rename columns
        rename_cols = {'count(Vehicle_ID)':'Sales_Volumn'}
        for i in rename_cols:
            dealersales_monthly = dealersales_monthly.withColumnRenamed(i, rename_cols[i])
        #merge dealer_sap_df    
        dealersales_monthly = dealersales_monthly.join(dealer_sap_df, 'SAP', 'inner')
        miss_sap = dealersales_monthly.join(dealer_sap_df.select(['SAP','GSSN']), 'SAP', 'left')
        #change 1933 back to 1931
        func_replace_char_back = udf(replace_char_back)
        miss_sap = miss_sap.withColumn('SAP', func_replace_char_back('SAP'))
        miss_sap = miss_sap.filter((miss_sap.GSSN.isnull())&(miss_sap.SAP.like("%1931%"))).drop_duplicates(['SAP']).select(['SAP']).toPandas()
        
        #original miss sap data
        spark.sql("""insert overwrite table revr_bmbs_srv_contract_blue.tgt_miss_sap_cesar_info 
        select * from src_cesar_info_2018 a where  a.sap is in ("""+miss_sap+")")
        
         #miss sap code
        spark.sql("""insert overwrite table revr_bmbs_srv_contract_blue.tgt_miss_sap_code 
        select distinct(a.sap) from revr_bmbs_srv_contract_blue.tgt_miss_sap_cesar_info a""")         

        return dealersales_monthly     
    

    
class Final_Contract:
    
    def __init__(self):
        pass
    
    def get_contract_esse(self,blue_contract_final,blue_balance,silver_contract_esse):
        
        blue_contract_final = blue_contract_final.join(blue_balance.select(['ContractNumber',
                                                                            'Year',
                                                                            'Cost_2015',
                                                                            'Cost_2016',
                                                                            'Cost_2017',
                                                                            'Cost_2018',
                                                                            'Cost_2019',
                                                                            'RemainBalance_2015',
                                                                            'RemainBalance_2016',
                                                                            'RemainBalance_2017',
                                                                            'RemainBalance_2018',
                                                                            'RemainBalance_2019']), 'ContractNumber','left')

        blue_contract_final = blue_contract_final.drop('Product')

        #need to add year relative colmuns and also the balance table columns like loc 5:  to be 6: and 6: to 7: when new year come    
        new_cols = ['Oil','CGI','DCCount','A_Count','B_Count','A_CFilter_Count',\
        'Spark_Plug_Count','ServiceTimes','A','B','A_C','SP','Detail_B','Detail_A/C',\
        'Detail_A','Detail_SP','RemainService','Total_Detail','Status','CostTillNow',\
        'Balance', 'Value_A','Value_B','Value_SP','Value_AC','TotalUpperLimit','OtherCost',\
        'Gap','ExceedAmount','Repair_Date','Days_To_Next_Service_Up','Days_To_Next_Service_Low',\
        'Next_Service_Date_Low','Next_Service_Date_Up','Year','Violate_Status']+blue_balance.columns[6:]
        
        for col in new_cols:
            silver_contract_esse = silver_contract_esse.withColumn(col,lit(''))
            
        silver_contract_esse = silver_contract_esse.drop('Product_Line')
            
        silver_contract_esse = silver_contract_esse.withColumnRenamed('Service_Type','Product_Line')
        
        silver_contract_esse = silver_contract_esse.select(['ContractNumber',
                                                             'Baumuster_6',
                                                             'Product_Line',
                                                             'Fin',
                                                             'Brand',
                                                             'Class',
                                                             'Model',
                                                             'First_Registration_Date',
                                                             'Signature_Date',
                                                             'Vehicle_Age',
                                                             'Product_EN',
                                                             'Value',
                                                             'Oil',
                                                             'CGI',
                                                             'DCCount',
                                                             'A_Count',
                                                             'B_Count',
                                                             'A_Cfilter_count',
                                                             'Spark_Plug_Count',
                                                             'GSSN',
                                                             'Dealer_Name_CN',
                                                             'Owner_Group',
                                                             'Region',
                                                             'Province',
                                                             'City',
                                                             'ServiceTimes',
                                                             'A',
                                                             'B',
                                                             'A_C',
                                                             'SP',
                                                             'Detail_B',
                                                             'Detail_A/C',
                                                             'Detail_A',
                                                             'Detail_SP',
                                                             'RemainService',
                                                             'Total_Detail',
                                                             'Status',
                                                             'CostTillNow',
                                                             'Balance',
                                                             'Value_A',
                                                             'Value_B',
                                                             'Value_SP',
                                                             'Value_AC',
                                                             'TotalUpperLimit',
                                                             'OtherCost',
                                                             'Gap',
                                                             'ExceedAmount',
                                                             'Repair_Date',
                                                             'Days_To_Next_Service_Up',
                                                             'Days_To_Next_Service_Low',
                                                             'Next_Service_Date_Low',
                                                             'Next_Service_Date_Up',
                                                             'Year',
                                                             'Cost_2015',
                                                             'Cost_2016',
                                                             'Cost_2017',
                                                             'Cost_2018',
                                                             'Cost_2019',
                                                             'RemainBalance_2015',
                                                             'RemainBalance_2016',
                                                             'RemainBalance_2017',
                                                             'RemainBalance_2018',
                                                             'RemainBalance_2019',
                                                             'Violate_Status'])

        contract_essence = blue_contract_final.unionAll(silver_contract_esse)
  
        contract_essence = contract_essence.select(['ContractNumber' ,
                                                    'Fin' ,
                                                    'Baumuster_6' ,
                                                    'Brand' ,
                                                    'Class' ,
                                                    'Model' ,
                                                    'First_Registration_Date' ,
                                                    'Signature_Date' ,
                                                    'Vehicle_Age' ,
                                                    'Product_EN' ,
                                                    'Product_Line' ,
                                                    'Value' ,
                                                    'Oil' ,
                                                    'CGI' ,
                                                    'DCCount' ,
                                                    'A_Count' ,
                                                    'B_Count' ,
                                                    'A_CFilter_Count' ,
                                                    'Spark_Plug_Count' ,
                                                    'GSSN' ,
                                                    'Dealer_Name_CN' ,
                                                    'Owner_Group' ,
                                                    'Region' ,
                                                    'Province' ,
                                                    'City' ,
                                                    'ServiceTimes' ,
                                                    'A' ,
                                                    'B' ,
                                                    'AC' ,
                                                    'SP' ,
                                                    'Total_Detail' ,
                                                    'RemainService' ,
                                                    'Status' ,
                                                    'CostTillNow' ,
                                                    'Balance' ,
                                                    'Value_A' ,
                                                    'Value_B' ,
                                                    'Value_SP' ,
                                                    'Value_AC' ,
                                                    'TotalUpperLimit' ,
                                                    'OtherCost' ,
                                                    'Gap' ,
                                                    'ExceedAmount' ,
                                                    'Repair_Date' ,
                                                    'Days_To_Next_Service_Up' ,
                                                    'Days_To_Next_Service_Low' ,
                                                    'Next_Service_Date_Up' ,
                                                    'Next_Service_Date_Low' ,
                                                    'Year' ,
                                                    'Cost_2015' ,
                                                    'Cost_2016' ,
                                                    'Cost_2017' ,
                                                    'Cost_2018' ,
                                                    'Cost_2019' ,
                                                    'RemainBalance_2015' ,
                                                    'RemainBalance_2016' ,
                                                    'RemainBalance_2017' ,
                                                    'RemainBalance_2018' ,
                                                    'RemainBalance_2019'])
        
        return contract_essence    
    
    
    
    def get_isp_contract_esse(self,isp_blue_contract_final,isp_blue_balance,isp_silver_contract_esse,contract):
        
        #need to add year relative colmuns and also the balance table columns like loc 5:  to be 6:
        isp_blue_contract_final = isp_blue_contract_final.join(isp_blue_balance.select(['ContractNumber',
                                                                            'Year',
                                                                            'Cost_2015',
                                                                            'Cost_2016',
                                                                            'Cost_2017',
                                                                            'Cost_2018',
                                                                            'Cost_2019',
                                                                            'RemainBalance_2015',
                                                                            'RemainBalance_2016',
                                                                            'RemainBalance_2017',
                                                                            'RemainBalance_2018',
                                                                            'RemainBalance_2019']), 'ContractNumber','left')


        isp_blue_contract_final = isp_blue_contract_final.drop('Product')
    
        new_cols = ['Oil','CGI','DCCount','A_Count','B_Count','A_CFilter_Count',\
        'Spark_Plug_Count','Rear_Bridge_Differential_Oil_count', \
        'Front_Brake_Friction_Plate_count','Brake_Fluid_count','Front_Wiper_Blade_count','Rear_Wiper_Blade_count',
        'ServiceTimes','A','B','A_C','SP','RBDO','FBFP','BF','FWB','RWB','Detail_B','Detail_A/C',\
        'Detail_A','Detail_SP','detail_BF','detail_RBDO','detail_FWB','detail_RWB',\
        'detail_FBFP','RemainService','Total_Detail','Status','CostTillNow',\
        'Balance', 'Value_A','Value_B','Value_SP','Value_AC','TotalUpperLimit','OtherCost',\
        'Gap','ExceedAmount','Repair_Date','Days_To_Next_Service_Up','Days_To_Next_Service_Low',\
        'Next_Service_Date_Low','Next_Service_Date_Up','Year','Violate_Status']+isp_blue_balance.columns[6:]
        
        for col in new_cols:
            isp_silver_contract_esse = isp_silver_contract_esse.withColumn(col,lit(''))
            
        isp_silver_contract_esse = isp_silver_contract_esse.drop('Product_Line')
            
        isp_silver_contract_esse = isp_silver_contract_esse.withColumnRenamed('Service_Type','Product_Line')
        
        isp_silver_contract_esse = isp_silver_contract_esse.select(['ContractNumber',
                                                             'Fin',
                                                             'Baumuster_6',
                                                             'Brand',
                                                             'Class',
                                                             'Model',
                                                             'First_Registration_Date',
                                                             'Signature_Date',
                                                             'Vehicle_Age',
                                                             'Product_EN',
                                                             'Product_Line',
                                                             'Value',
                                                             'Oil',
                                                             'CGI',
                                                             'DCCount',
                                                             'A_Count',
                                                             'B_Count',
                                                             'A_Cfilter_count',
                                                             'Spark_Plug_Count',
                                                             'Rear_Bridge_Differential_Oil_count',
                                                             'Front_Brake_Friction_Plate_count',
                                                             'Brake_Fluid_count',
                                                             'Front_Wiper_Blade_count',
                                                             'Rear_Wiper_Blade_count',
                                                             'GSSN',
                                                             'Dealer_Name_CN',
                                                             'Owner_Group',
                                                             'Region',
                                                             'Province',
                                                             'City',                                                         
                                                             'ServiceTimes',
                                                             'A',
                                                             'B',
                                                             'A_C',
                                                             'SP',
                                                             'RBDO',
                                                             'FBFP',
                                                             'BF',
                                                             'FWB',
                                                             'RWB',
                                                             'Total_Detail',
                                                             'RemainService',
                                                             'Status',
                                                             'CostTillNow',
                                                             'Balance',
                                                             'Value_A',
                                                             'Value_AMG_A',
                                                             'Value_B',
                                                             'Value_AMG_B',
                                                             'Value_AC',
                                                             'Value_SP',
                                                             'Value_RBDO',
                                                             'Value_FBFP',
                                                             'Value_BF',
                                                             'Value_FWB', 
                                                             'Value_RWB',
                                                             'Unavailable_Cost',
                                                             'OtherCost',
                                                             'TotalUpperLimit',
                                                             'Gap',
                                                             'ExceedAmount',
                                                             'Repair_Date',
                                                             'Days_To_Next_Service_Up',
                                                             'Days_To_Next_Service_Low',
                                                             'Next_Service_Date_Low',
                                                             'Next_Service_Date_Up',
                                                             'Year',
                                                             'Cost_2015',
                                                             'Cost_2016',
                                                             'Cost_2017',
                                                             'Cost_2018',                                                             
                                                             'Cost_2019',
                                                             'RemainBalance_2015',
                                                             'RemainBalance_2016',
                                                             'RemainBalance_2017',
                                                             'RemainBalance_2018',
                                                             'RemainBalance_2019',
                                                             'Violate_Status'])

        isp_blue_contract_final = isp_blue_contract_final.select([col('isp_blue_contract_final.'+a) for a in isp_silver_contract_esse.columns])

        isp_contract_essence = isp_blue_contract_final.unionAll(isp_silver_contract_esse)
        isp_contract_essence = isp_contract_essence.join(contract.select(['ContractNumber','Activation_date']),'ContractNumber','left')
        isp_contract_essence = isp_contract_essence.drop('Violate_Status')

        return isp_contract_essence
















#
