# -*- coding: utf-8 -*-
"""
Created on Fri May 25 11:48:00 2018

@author: jiajuwu
"""
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame
from pyspark.sql.types import FloatType,StringType
from pyspark.sql.functions import lit,udf,dense_rank,array,col,explode,struct
from pyspark.sql.window import Window
from typing import Iterable
from typing import Iterable
from pyspark.sql.functions import dense_rank, ntile
from pyspark.sql.functions import col
#from pyspark.sql import functions as F

#import svcb.ETL as etl

def get_diff_label(diff):
    
    if diff:
        if diff <= 0 and diff > -50:
            Label = 'Exceed 0-50'
        if diff <= -50 and diff > -100:
            Label = 'Exceed 50-100'
        if diff <= -100 and diff > -200:
            Label = 'Exceed 100-200'
        if diff <= -200 and diff > -300:
            Label = 'Exceed 200-300'
        if diff <= -300:
            Label = 'Exceed Over 300'
        if diff > 0:
            Label = 'Not-Exceed'
    else:
        Label = 'Other Item'
    
    return Label 



def get_isp_diff_label(diff):
    
    if diff:
        if diff <= 0 and diff > -50:
            Label = 'Exceed 0-50'
        if diff <= -50 and diff > -100:
            Label = 'Exceed 50-100'
        if diff <= -100 and diff > -200:
            Label = 'Exceed 100-200'
        if diff <= -200 and diff > -300:
            Label = 'Exceed 200-300'
        if diff <= -300:
            Label = 'Exceed Over 300'
        if diff > 0:
            Label = 'Not-Exceed'
        if diff ==-10:
            Label = 'Not Available'
    else:
        Label = 'Other Item'
    
    return Label 



def get_excamount(gap):
    
    if gap:
        if gap < 0 and gap > -50:
            Exceedamount = 'Exceed 0-50'
        if gap <= -50 and gap > -100:
            Exceedamount = 'Exceed 50-100'
        if gap <= -100 and gap > -200:
            Exceedamount = 'Exceed 100-200'
        if gap <= -200 and gap > -300:
            Exceedamount = 'Exceed 200-300'
        if gap <= -300:
            Exceedamount = 'Exceed Over 300'
        if gap >= 0:
            Exceedamount = 'Not-Exceed'
    else:
        Exceedamount = ''
        
    return Exceedamount  

def melt(
        df: DataFrame, 
        id_vars: Iterable[str], value_vars: Iterable[str], 
        var_name: str="variable", value_name: str="value") -> DataFrame:
    """Convert :class:`DataFrame` from wide to long format."""

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name)) 
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))
        
    cols = id_vars + [
            col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)


def union(*dfs):
    return reduce(DataFrame.unionAll, dfs)







def total_upperlimit_cal(value_a,value_amg_a,value_b,value_amg_b,value_filter,value_sparkplug,value_rbdo,value_fbfp,value_bf,value_fwb,value_rwb):
    price_list=[value_a,value_amg_a,value_b,value_amg_b,value_filter,value_sparkplug,value_rbdo,value_fbfp,value_bf,value_fwb,value_rwb]
    total_upperlimit=0
    for i in price_list:
        if i == -10 :
            i =0
        else:
            i=i
            total_upperlimit+=i   
    return total_upperlimit    
        
def pric_status(value_a,value_amg_a,value_b,value_amg_b,value_filter,value_sparkplug,value_rbdo,value_fbfp,value_bf,value_fwb,value_rwb):
    price_list=[value_a,value_amg_a,value_b,value_amg_b,value_filter,value_sparkplug,value_rbdo,value_fbfp,value_bf,value_fwb,value_rwb]
    status=''
    for i in price_list:
        if i ==-10:
            tmp_status='Not Available'
        else:
            tmp_status='Available'
        status+=tmp_status
    if status.str.contains('Not'):
        status_end='Not Available'
    else:
         status_end='Available'
    return status_end  
          
def damage_code_na_union(value_a,value_amg_a,value_b,value_amg_b,value_filter,value_sparkplug,value_rbdo,value_fbfp,value_bf,value_fwb,value_rwb):
    damage_dic={value_a:'00011SV',value_amg_a:'00011SV',value_b:'00012SV',value_amg_b:'00012SV',value_filter:'83108SV',value_sparkplug:'15031SV',value_rbdo:'3500NSV',value_fbfp:'42114D1',value_bf:'43000SV',value_fwb:'82023SV',value_rwb:'82063SV'}
    union_damage=''
    for a in damage_dic:
        if a ==-10:
            tmp_damage=damage_dic[a]
        else:
            tmp_damage=''
        union_damage=union_damage+'*'+tmp_damage          
    return union_damage

def filter_damage_code(union_damage,damage):
    union_list=union_damage.split('*')
    if damage in union_list:
        damage_status='Not Available'
    else:
        damage_status='Available'
    return damage_status


def filter_damage_code(union_damage,damage):
    union_list=union_damage.split('*')
    if damage in union_list:
        damage_status='Not Available'
    else:
        damage_status='Available'
    return damage_status

def label_violate_status(Vehicle_Age):
    if Vehicle_Age >= 1080:
        violate_status = 'Not Violate'
    else:
        violate_status = 'Violate'
    return  violate_status  



class Cost_Control:
    
    def __init__(self,spark_context):
        self.sc = spark_context
    """
    cost control of claim for each service
    for example:
        1A+1B each service has its own upperlimit for cost of claim
        Normally, dealers need to claim costs below upperlimit, 
        but some will claim more which requires more attention to be controlled 
    """
    def _get_claim_control(self,blue_contract_esse,blue_cost_esse,silver_contract_esse,silver_cost_esse,gssn_workshop):                

        rename_string_text = """baumuster_6, 
        service_type as product_line, 
        value_a as 00012SV, 
        value_b as 00011SV, 
        value_sp as 15031SV, 
        value_ac as 83108SV"""
        
                
        price_blue = spark.sql("select " + rename_string_text + \
        " from revr_bmbs_srv_contract_blue.src_price_blue_info")
        
        price_blue_amg = spark.sql("select " + rename_string_text + \
        " from revr_bmbs_srv_contract_blue.src_price_blue_amg_info")
        
        price_bluelight = spark.sql("select " + rename_string_text + \
        " from revr_bmbs_srv_contract_blue.src_price_bluelight_info")
        
        price_bluelight_amg = spark.sql("select " + rename_string_text + \
        " from revr_bmbs_srv_contract_blue.src_price_bluelight_amg_info")

        blue_contract_info = blue_contract_esse.join(gssn_workshop.select(['GSSN',\
        'Workshop']), 'GSSN','left')        
        
        blue_contract_info = blue_contract_info.select(['ContractNumber',
                                                        'SignatureDate',
                                                        'Fin',
                                                        'Baumuster_6',
                                                        'Brand',
                                                        'Class',
                                                        'Model',
                                                        'Product_EN',
                                                        'Service_Type',
                                                        'Oil',
                                                        'GSSN',
                                                        'Workshop',
                                                        'Dealer_Name_CN',
                                                        'Owner_Group',
                                                        'Region',
                                                        'Province',
                                                        'City',
                                                        'Total_Detail'])

        rename_cols = {'Workshop':'ServiceWorkshop','Dealer_Name_CN':'ServiceDealer_CN',\
        'Owner_Group':'ServiceGroup','Province':'ServiceProvince','City':'ServiceCity'}
        for i in rename_cols:
            blue_cost_esse = blue_cost_esse.withColumnRenamed(i,rename_cols[i])
 
        blue_cost_info = blue_cost_esse.join(blue_contract_info.select(['ContractNumber',\
        'Fin','Baumuster_6','Brand','Class','Model','Oil','Workshop','Dealer_Name_CN',\
        'Owner_Group','Region','Province','City']), 'ContractNumber','left')    

        
#        blue_cost_info = blue_cost_info.drop('product_line')
        
        blue_cost_info = blue_cost_info.withColumnRenamed('Service_Type','Product_Line')


        """
        calculate difference between value and claim amount based on different product_en and oil 
        """
        print("calculate difference")
        blue_claim = blue_cost_info.filter((blue_cost_info.Product_EN == 'GMP') & (blue_cost_info.Oil == 'Normal'))
        blue_amg_claim = blue_cost_info.filter((blue_cost_info.Product_EN == 'GMP') & (blue_cost_info.Oil == 'AMG'))
        
        bluelight_claim = blue_cost_info.filter((blue_cost_info.Product_EN == 'GMP light') & (blue_cost_info.Oil == 'Normal'))
        bluelight_amg_claim = blue_cost_info.filter((blue_cost_info.Product_EN == 'GMP light') & (blue_cost_info.Oil == 'AMG'))    
            

           
        #covert dataframe from wide to long format
        print("convert dataframe from wide to long format")
        id_vars_param = ['Baumuster_6','Product_Line']
        value_vars_param = ['00012SV','00011SV','15031SV','83108SV']
        var_name_param = 'DamageCode'
        value_name_param = 'UpperLimit'
        
        
        """
        blue_claim
        """
        print("converting blue_claim")
        price_blue = melt(price_blue, id_vars=id_vars_param,\
                          value_vars=value_vars_param,var_name=var_name_param,\
                          value_name=value_name_param)
        
        blue_claim = blue_claim.join(price_blue,['Baumuster_6','Product_Line', 'DamageCode'],'left')
        
        #calculate difference between standard value and real claim amount for each service
        blue_claim = blue_claim.withColumn('Diff',(blue_claim.UpperLimit - blue_claim.Claim_Amount))

        
        """
        blue_amg_claim
        """
        print("converting blue_amg_claim")
        price_blue_amg = melt(price_blue_amg, id_vars=id_vars_param,\
                              value_vars=value_vars_param,var_name=var_name_param,\
                              value_name=value_name_param)
        
        blue_amg_claim = blue_amg_claim.join(price_blue_amg,['Baumuster_6','Product_Line', 'DamageCode'],'left')
        
        #calculate difference between standard value and real claim amount for each service
        blue_amg_claim = blue_amg_claim.withColumn('Diff',(blue_amg_claim.UpperLimit - blue_amg_claim.Claim_Amount))
        
        """
        bluelight_claim
        """
        print("converting bluelight_claim")        
        price_bluelight = melt(price_bluelight, id_vars=id_vars_param,\
                              value_vars=value_vars_param,var_name=var_name_param,\
                              value_name=value_name_param)
        
        bluelight_claim = bluelight_claim.join(price_bluelight,['Baumuster_6','Product_Line', 'DamageCode'],'left')
        
        #calculate difference between standard value and real claim amount for each service
        bluelight_claim = bluelight_claim.withColumn('Diff',(bluelight_claim.UpperLimit - bluelight_claim.Claim_Amount))        

        """
        bluelight_amg_claim
        """
        print("converting bluelight_amg_claim")        
        price_bluelight_amg = melt(price_bluelight_amg, id_vars=id_vars_param,\
                              value_vars=value_vars_param,var_name=var_name_param,\
                              value_name=value_name_param)
        
        bluelight_amg_claim = bluelight_amg_claim.join(price_bluelight_amg,['Baumuster_6','Product_Line', 'DamageCode'],'left')
        
        #calculate difference between standard value and real claim amount for each service
        bluelight_amg_claim = bluelight_amg_claim.withColumn('Diff',(bluelight_amg_claim.UpperLimit - bluelight_amg_claim.Claim_Amount))    
        
        """
        merge all
        """
        print("merge all")
        blue_blue_claim =  union(blue_claim,blue_amg_claim)
        blue_blue_claim = blue_blue_claim.withColumn('Violate_Status',lit(''))
        
        func_label_violate_status = udf(label_violate_status, StringType())           
        blue_light_claim = union(bluelight_amg_claim,bluelight_claim)
        blue_light_claim = blue_light_claim.withColumn('Violate_Status',func_label_violate_status('Vehicle_Age'))
        
        blue_claim_all = union(blue_blue_claim,blue_light_claim)
        
        #classify diff based on rules to generate label
        func_get_diff_label = udf(get_diff_label, StringType())
        blue_claim_all = blue_claim_all.withColumn('Label', func_get_diff_label('Diff'))
        blue_claim_all = blue_claim_all.drop('Product')

        silver_contract_info = silver_contract_esse.join(gssn_workshop.select(['GSSN',\
        'Workshop']), 'GSSN','left')        

        silver_contract_info = silver_contract_info.select(['ContractNumber',
                                                        'SignatureDate',
                                                        'Fin',
                                                        'Baumuster_6',
                                                        'Brand',
                                                        'Class',
                                                        'Model',
                                                        'Product_EN',
                                                        'Service_Type',
                                                        'GSSN',
                                                        'Workshop',
                                                        'Dealer_Name_CN',
                                                        'Owner_Group',
                                                        'Region',
                                                        'Province',
                                                        'City'])


        silver_cost_esse = silver_cost_esse.withColumn('Oil',lit(''))
        silver_cost_esse = silver_cost_esse.withColumn('Total_Detail',lit(''))
        silver_cost_esse = silver_cost_esse.withColumn('Violate_Status',lit(''))
        silver_cost_esse = silver_cost_esse.withColumnRenamed('FIN','Fin').withColumnRenamed('VegaCredit_Date','Vegacredit_Date')
        silver_cost_esse = silver_cost_esse.select(['Baumuster_6',
                                                     'Product_Line',
                                                     'DamageCode',
                                                     'Total_Detail',
                                                     'ContractNumber',
                                                     'Claimno',
                                                     'Product_EN',
                                                     'Claim_Amount',
                                                     'Claim_Date',
                                                     'Repair_Date',
                                                     'First_Registration_Date',
                                                     'Vegacredit_Date',
                                                     'Mileage_When_Repair',
                                                     'Repair_Age',
                                                     'Mileage_Per_Day',
                                                     'Workshop',
                                                     'ServiceDealer_CN',
                                                     'ServiceGroup',
                                                     'ServiceRegion',
                                                     'Province',
                                                     'City',
                                                     'Oil',
                                                     'UpperLimit',
                                                     'Diff',
                                                     'Label',
                                                     'Violate_Status'])

        rename_cols = {'Workshop':'ServiceWorkshop','Province':'ServiceProvince','City':'ServiceCity'}
        for i in rename_cols:
            silver_cost_esse = silver_cost_esse.withColumnRenamed(i,rename_cols[i])

        silver_claim_all = silver_cost_esse.join(silver_contract_info.select(['ContractNumber',\
        'Fin','Brand','Class','Model','Oil','Workshop','Dealer_Name_CN',\
        'Owner_Group','Region','Province','City']), 'ContractNumber','left')    

        silver_claim_all = silver_claim_all.select(['Baumuster_6',
                                                       'Product_Line',
                                                       'DamageCode',
                                                       'Total_Detail',
                                                       'ContractNumber',
                                                       'SignatureDate',
                                                       'Claimno',
                                                       'Product_EN',
                                                       'Claim_Amount',
                                                       'Claim_Date',
                                                       'Repair_Date',
                                                       'First_Registration_Date',
                                                       'VegaCredit_Date',
                                                       'Mileage_When_Repair',
                                                       'Repair_Age',
                                                       'Mileage_Per_Day',
                                                       'ServiceWorkshop',
                                                       'ServiceDealer_CN',
                                                       'ServiceGroup',
                                                       'ServiceRegion',
                                                       'ServiceProvince',
                                                       'ServiceCity',
                                                       'Workshop',
                                                       'Dealer_Name_CN',
                                                       'Owner_Group',
                                                       'Region',
                                                       'Province',
                                                       'City',
                                                       'Fin',
                                                       'Brand',
                                                       'Class',
                                                       'Model',
                                                       'Oil',
                                                       'UpperLimit',
                                                       'Diff',  
                                                       'Label',
                                                       'Violate_Status'])

        blue_claim_all = blue_claim_all.select(['Baumuster_6',
                                                       'Product_Line',
                                                       'DamageCode',
                                                       'Total_Detail',
                                                       'ContractNumber',
                                                       'SignatureDate',
                                                       'Claimno',
                                                       'Product_EN',
                                                       'Claim_Amount',
                                                       'Claim_Date',
                                                       'Repair_Date',
                                                       'First_Registration_Date',
                                                       'VegaCredit_Date',
                                                       'Mileage_When_Repair',
                                                       'Repair_Age',
                                                       'Mileage_Per_Day',
                                                       'ServiceWorkshop',
                                                       'ServiceDealer_CN',
                                                       'ServiceGroup',
                                                       'ServiceRegion',
                                                       'ServiceProvince',
                                                       'ServiceCity',
                                                       'Workshop',
                                                       'Dealer_Name_CN',
                                                       'Owner_Group',
                                                       'Region',
                                                       'Province',
                                                       'City',
                                                       'Fin',
                                                       'Brand',
                                                       'Class',
                                                       'Model',
                                                       'Oil',
                                                       'UpperLimit',
                                                       'Diff',  
                                                       'Label',
                                                       'Violate_Status'])

        claim_all = blue_claim_all.unionAll(silver_claim_all)

        claim_all = claim_all.select(['Baumuster_6',
                                       'Product_Line',
                                       'DamageCode',
                                       'Total_Detail',
                                       'ContractNumber',
                                       'Claimno',
                                       'Product_EN',
                                       'Claim_Amount',
                                       'Claim_Date',
                                       'Repair_Date',
                                       'First_Registration_Date',
                                       'VegaCredit_Date',
                                       'Mileage_When_Repair',
                                       'Repair_Age',
                                       'Mileage_Per_Day',
                                       'ServiceWorkshop',
                                       'ServiceDealer_CN',
                                       'ServiceGroup',
                                       'ServiceRegion',
                                       'ServiceProvince',
                                       'ServiceCity',
                                       'Workshop',
                                       'Dealer_Name_CN',
                                       'Owner_Group',
                                       'Region',
                                       'Province',
                                       'City',
                                       'Fin',
                                       'Brand',
                                       'Class',
                                       'Model',
                                       'Oil',
                                       'UpperLimit',
                                       'Diff',  
                                       'Label',
                                       'Violate_Status',
                                       'SignatureDate'])
        
        w =  Window.partitionBy(claim_all.ContractNumber,claim_all.DamageCode).orderBy(claim_all.Repair_Date)                                   
        claim_all = claim_all.withColumn('Damage_Sort',dense_rank().over(w))
        
        return claim_all
    

    def _get_isp_claim_control(self,isp_blue_contract_esse,isp_blue_cost_esse,isp_silver_contract_esse,isp_silver_cost_esse,gssn_worshop,dealer_info):                

        isp_price = spark.sql("select * from revr_bmbs_srv_contract_blue.src_isp_claim_price_info")

        cols = StructType([StructField('Model',StringType(),True),
                            StructField('Baumuster_6',StringType(),True),
                            StructField('Service_Type',StringType(),True),
                            StructField('DamageCode',StringType(),True),
                            StructField('Part_Total',StringType(),True),
                            StructField('Labor_Rate',StringType(),True)])  
        isp_price = spark.createDataFrame(isp_price.rdd,cols)

        isp_price_na_list = ['Part_Total','Labor_Rate']
        isp_price_na_dict = {i:-10 for i in isp_price_na_list}
        isp_price = isp_price.na.fill(isp_price_na_dict)


        isp_blue_contract_info = isp_blue_contract_esse.join(gssn_worshop.select(['GSSN',\
        'Workshop']), 'GSSN','left')             
  
        isp_blue_contract_info = isp_blue_contract_info.select(['ContractNumber',
                                                                'Signature_Date',        
                                                                'Fin',
                                                                'Baumuster_6',
                                                                'Brand',
                                                                'Class',
                                                                'Model',
                                                                'Product_EN',
                                                                'Service_Type',
                                                                'Oil',
                                                                'GSSN',
                                                                'Workshop',
                                                                'Dealer_Name_CN',
                                                                'Owner_Group',
                                                                'Region',
                                                                'Province',
                                                                'City',
                                                                'Total_Detail'])
        
        #productline means servicetype
        isp_blue_cost_info = isp_blue_cost_esse.select(['ContractNumber',
                                                          'Claimno',
                                                          'Repair_Date',
                                                          'Product_EN',
                                                          'Product_line',
                                                          'DamageCode',
                                                          'Claim_Amount',
                                                          'Claim_Date',
                                                          'First_Registration_Date',
                                                          'VegaCredit_Date',
                                                          'Mileage_When_Repair',
                                                          'Repair_Age',
                                                          'Mileage_Per_Day',                                          
                                                          'Workshop',
                                                          'ServiceDealer_CN',
                                                          'ServiceGroup',
                                                          'ServiceRegion',
                                                          'Province',
                                                          'City'])


        rename_cols = {'Workshop':'ServiceWorkshop','Dealer_Name_CN':'ServiceDealer_CN',\
        'Owner_Group':'ServiceGroup','Province':'ServiceProvince','City':'ServiceCity'}
        for i in rename_cols:
            isp_blue_cost_info = isp_blue_cost_info.withColumnRenamed(i,rename_cols[i])
 
        
        #have change detail to Total_Detail
        isp_blue_cost_info = isp_blue_cost_info.join(isp_blue_contract_info.select(['ContractNumber',\
        'Fin','Baumuster_6','Brand','Class','Model','Oil','Workshop','Dealer_Name_CN',\
        'Owner_Group','Region','Province','City','Total_Detail','Signature_Date']), 'ContractNumber','left')    

        sql_text_isp_cost_act="""select concat(internal_vega_claim_number,'_',country_code_vega) vega_code,
        concat(fault_location_id,fault_type_id) as damagecode,
        repair_date,
        material_costs_net,
        wage_costs,
        total_costs 
        from datalake_aqua.dw_vhcl_aqua_claimes_overall"""
        isp_cost_act=spark.sql(sql_text_isp_cost_act)
        
        cols = StructType([StructField('Claimno',StringType(),True),
                           StructField('DamageCode',StringType(),True),
                           StructField('Repair_Date',StringType(),True),           
                           StructField('Part_Cost',StringType(),True),
                           StructField('Labor_Cost',StringType(),True),
                           StructField('Total_Cost',StringType(),True)])  
        isp_cost_act = spark.createDataFrame(isp_cost_act.rdd,cols)

        isp_cost_join=isp_blue_cost_info.join(isp_cost_act.select(['Claimno','DamageCode','Repair_Date','Labor_Cost','Part_Cost','Total_Cost']),['Claimno','DamageCode','Repair_Date'],'left')
 
        #remain the model from contract，drop the model from price table
        isp_cost_join_amg=isp_cost_join.filter(isp_cost_join.Brand=='AMG')
        isp_price_amg=isp_price.filter(isp_price.Service_Type.like('%AMG%'))
        isp_cost_amg_final=isp_cost_join_amg.join(isp_price_amg.select(['Baumuster_6','Damagecode','Labor_Rate','Part_Total']),['Baumuster_6','Damagecode'],'left')
        
        isp_cost_join_not_amg=isp_cost_join.filter(isp_cost_join.Brand != 'AMG')
        isp_price_not_amg=isp_price.filter(~isp_price.Service_Type.like('%AMG%'))
        isp_cost_not_amg_final=isp_cost_join_not_amg.join(isp_price_not_amg.select(['Baumuster_6','Damagecode','Labor_Rate','Part_Total']),['Baumuster_6','Damagecode'],'left')
        
        isp_cost_final=isp_cost_amg_final.union(isp_cost_not_amg_final)

        isp_cost_fill_na_list = ['Labor_Rate','Labor_Cost','Part_Total','Part_Cost']
        isp_cost_fill_na_dict = {i:-10 for i in isp_cost_fill_na_list}
        isp_cost_final = isp_cost_final.na.fill(isp_cost_fill_na_dict)
         
        isp_cost_final_na=isp_cost_final.filter((isp_cost_final.Labor_Rate==-10)|(isp_cost_final.Part_Total==-10))
        isp_cost_fill_lpt_na_list = ['Labor_Diff','Part_Diff','Total_Diff','Total_Rate']
        for ispna in isp_cost_fill_lpt_na_list:
            isp_cost_final_na = isp_cost_final_na.withColumn(ispna,lit(-10))
        
        isp_cost_final_n = isp_cost_final.filter((isp_cost_final.Labor_Rate!=-10)&(isp_cost_final.Part_Total!=-10))        
        isp_cost_final_n = isp_cost_final_n.withColumn('Total_Rate',(isp_cost_final_n.Labor_Rate + isp_cost_final_n.Part_Total))
        isp_cost_final_n = isp_cost_final_n.withColumn('Labor_Diff',(isp_cost_final_n.Labor_Rate - isp_cost_final_n.Labor_Cost))
        isp_cost_final_n = isp_cost_final_n.withColumn('Part_Diff',(isp_cost_final_n.Part_Total - isp_cost_final_n.Part_Cost))
        isp_cost_final_n = isp_cost_final_n.withColumn('Total_Diff',(isp_cost_final_n.Total_Rate - isp_cost_final_n.Claim_Amount))

        isp_cost_final=isp_cost_final_na.union(isp_cost_final_n)
        isp_cost_final_nn=isp_cost_final.filter((isp_cost_final.Labor_Rate!=-10)&(isp_cost_final.Part_Total!=-10))

        isp_total_claim_lr = isp_cost_final_nn.groupby('ClaimNo').agg({'Labor_Rate':'sum'}).withColumnRenamed('sum(Labor_Rate)','Labor_Rate_Total')        
        isp_total_claim_lc = isp_cost_final_nn.groupby('ClaimNo').agg({'Labor_Cost':'sum'}).withColumnRenamed('sum(Labor_Cost)','Labor_Cost_Total')
        isp_total_claim_ld = isp_cost_final_nn.groupby('ClaimNo').agg({'Labor_Diff':'sum'}).withColumnRenamed('sum(Labor_Diff)','Labor_Diff_Total')        

        isp_total_claim_pt = isp_cost_final_nn.groupby('ClaimNo').agg({'Part_Total':'sum'}).withColumnRenamed('sum(Part_Total)','Part_Total_Total')
        isp_total_claim_pc = isp_cost_final_nn.groupby('ClaimNo').agg({'Part_Cost':'sum'}).withColumnRenamed('sum(Part_Cost)','Part_Cost_Total')        
        isp_total_claim_pd = isp_cost_final_nn.groupby('ClaimNo').agg({'Part_Diff':'sum'}).withColumnRenamed('sum(Part_Diff)','Part_Diff_Total')        

        isp_total_claim_tr = isp_cost_final_nn.groupby('ClaimNo').agg({'Total_Rate':'sum'}).withColumnRenamed('sum(Total_Rate)','Total_Rate_Total')
        isp_total_claim_ca = isp_cost_final_nn.groupby('ClaimNo').agg({'Claim_Amount':'sum'}).withColumnRenamed('sum(Claim_Amount)','ClaimAmount_Total')
        isp_total_claim_td = isp_cost_final_nn.groupby('ClaimNo').agg({'Total_Diff':'sum'}).withColumnRenamed('sum(Total_Diff)','Total_Diff_Total')

        isp_cost_final_nn = isp_cost_final_nn.join(isp_total_claim_lr,  'ClaimNo', 'left')
        isp_cost_final_nn = isp_cost_final_nn.join(isp_total_claim_lc,  'ClaimNo', 'left')
        isp_cost_final_nn = isp_cost_final_nn.join(isp_total_claim_ld,  'ClaimNo', 'left')
        isp_cost_final_nn = isp_cost_final_nn.join(isp_total_claim_pt,  'ClaimNo', 'left')
        isp_cost_final_nn = isp_cost_final_nn.join(isp_total_claim_pc,  'ClaimNo', 'left')
        isp_cost_final_nn = isp_cost_final_nn.join(isp_total_claim_pd,  'ClaimNo', 'left')
        isp_cost_final_nn = isp_cost_final_nn.join(isp_total_claim_tr,  'ClaimNo', 'left')
        isp_cost_final_nn = isp_cost_final_nn.join(isp_total_claim_ca,  'ClaimNo', 'left')
        isp_cost_final_nn = isp_cost_final_nn.join(isp_total_claim_td, 'ClaimNo', 'left')

        isp_cost_final_naa=isp_cost_final.filter((isp_cost_final.Labor_Rate==-10)|(isp_cost_final.Part_Total==-10))

        naa_columns=['Labor_Rate_Total','Labor_Cost_Total','Labor_Diff_Total','Part_Total_Total','Part_Cost_Total','Part_Diff_Total','Total_Rate_Total','ClaimAmount_Total','Total_Diff_Total']
        for naac in naa_columns:
            isp_cost_final_naa=isp_cost_final_naa.withColumn(naac,lit(-10))
            
        isp_cost_final=isp_cost_final_nn.union(isp_cost_final_naa)    

        func_isp_get_excamount = udf(get_isp_diff_label, StringType())
        isp_cost_final = isp_cost_final.withColumn('Label',func_isp_get_excamount('Total_Diff'))

        isp_silver_contract_info = isp_silver_contract_esse.join(gssn_workshop.select(['GSSN',\
        'Workshop']), 'GSSN','left')        

        isp_silver_naa_columns=['Labor_Rate','Labor_Cost','Labor_Diff','Part_Total','Part_Cost','Part_Diff',\
                            'Total_Rate','Total_Cost','Total_Diff','Detail','Oil','Labor_Rate_Total',\
                            'Labor_Cost_Total','Labor_Diff_Total','Part_Total_Total','Part_Cost_Total',\
                            'Part_Diff_Total','Total_Rate_Total','ClaimAmount_Total','Total_Diff_Total']  
        
        for islnaac in isp_silver_naa_columns:
            isp_silver_contract_esse=isp_silver_contract_esse.withColumn(islnaac,lit(''))        
        
        isp_silver_contract_esse = isp_silver_contract_esse.join(gssn_worshop.select(['GSSN',\
        'Workshop']), 'GSSN','left')               
            
        isp_silver_contract_info = isp_silver_contract_esse.select(['ContractNumber',
                                                        'Signature_Date',
                                                        'Fin',
                                                        'Baumuster_6',
                                                        'Brand',
                                                        'Class',
                                                        'Model',
                                                        'Product_EN',
                                                        'Service_Type',
                                                        'Oil',
                                                        'GSSN',
                                                        'Workshop',
                                                        'Dealer_Name_CN',
                                                        'Owner_Group',
                                                        'Region',
                                                        'Province',
                                                        'City',
                                                        'Detail',
                                                        'Labor_Rate',
                                                        'Labor_Cost',
                                                        'Labor_Diff',
                                                        'Part_Total',
                                                        'Part_Cost',
                                                        'Part_Diff',
                                                        'Total_Rate',
                                                        'Total_Cost',
                                                        'Total_Diff',
                                                        'Labor_Rate_Total',
                                                        'Labor_Cost_Total',
                                                        'Labor_Diff_Total',
                                                        'Part_Total_Total',
                                                        'Part_Cost_Total',
                                                        'Part_Diff_Total',
                                                        'Total_Rate_Total',
                                                        'ClaimAmount_Total',
                                                        'Total_Diff_Total'])
        
        isp_silver_cost_esse = isp_silver_cost_esse.withColumn('Oil',lit(''))
        isp_silver_cost_esse = isp_silver_cost_esse.withColumn('Total_Detail',lit(''))
        isp_silver_cost_esse = isp_silver_cost_esse.withColumn('Label',lit(''))        
        isp_silver_cost_esse = isp_silver_cost_esse.withColumnRenamed('FIN','Fin').withColumnRenamed('VegaCredit_Date','Vegacredit_Date')

        isp_silver_cost_esse = isp_silver_cost_esse.select(['Product_Line',
                                                     'DamageCode',
                                                     'Total_Detail',
                                                     'ContractNumber',
                                                     'Claimno',
                                                     'Product_EN',
                                                     'Claim_Amount',
                                                     'Claim_Date',
                                                     'Repair_Date',
                                                     'First_Registration_Date',
                                                     'Vegacredit_Date',
                                                     'Mileage_When_Repair',
                                                     'Repair_Age',
                                                     'Mileage_Per_Day',
                                                     'Workshop',
                                                     'ServiceDealer_CN',
                                                     'ServiceGroup',
                                                     'ServiceRegion',
                                                     'Province',
                                                     'City',
                                                     'Oil',
                                                     'Label'])

        rename_cols = {'Workshop':'ServiceWorkshop','Province':'ServiceProvince','City':'ServiceCity'}
        for i in rename_cols:
            isp_silver_cost_esse = isp_silver_cost_esse.withColumnRenamed(i,rename_cols[i])

        isp_silver_claim_all = isp_silver_cost_esse.join(isp_silver_contract_info.select(['ContractNumber',\
        'Signature_Date','Fin','Brand','Class','Model','Baumuster_6','Workshop','Dealer_Name_CN',\
        'Owner_Group','Region','Province','City','Labor_Rate','Labor_Cost','Labor_Diff','Part_Total',\
        'Part_Cost','Part_Diff','Total_Rate','Total_Diff','Total_Cost','Labor_Rate_Total','Labor_Cost_Total',\
        'Labor_Diff_Total','Part_Total_Total','Part_Cost_Total','Part_Diff_Total','Total_Rate_Total',\
        'ClaimAmount_Total','Total_Diff_Total']), 'ContractNumber','left')
            
        isp_silver_claim_all = isp_silver_claim_all.select(['Baumuster_6',
                                                       'Product_Line',
                                                       'DamageCode',
                                                       'Total_Detail',
                                                       'ContractNumber',
                                                       'Signature_Date',
                                                       'Claimno',
                                                       'Product_EN',
                                                       'Claim_Amount',
                                                       'Claim_Date',
                                                       'Repair_Date',
                                                       'First_Registration_Date',
                                                       'Vegacredit_Date',
                                                       'Mileage_When_Repair',
                                                       'Repair_Age',
                                                       'Mileage_Per_Day',
                                                       'ServiceWorkshop',
                                                       'ServiceDealer_CN',
                                                       'ServiceGroup',
                                                       'ServiceRegion',
                                                       'ServiceProvince',
                                                       'ServiceCity',
                                                       'Workshop',
                                                       'Dealer_Name_CN',
                                                       'Owner_Group',
                                                       'Region',
                                                       'Province',
                                                       'City',
                                                       'Fin',
                                                       'Brand',
                                                       'Class',
                                                       'Model',
                                                       'Oil',
                                                       'Labor_Rate',
                                                       'Labor_Cost',
                                                       'Labor_Diff',
                                                       'Part_Total',
                                                       'Part_Cost',
                                                       'Part_Diff',
                                                       'Total_Rate',
                                                       'Total_Cost',
                                                       'Total_Diff',
                                                       'Labor_Rate_Total',
                                                       'Labor_Cost_Total',
                                                       'Labor_Diff_Total',
                                                       'Part_Total_Total',
                                                       'Part_Cost_Total',
                                                       'Part_Diff_Total',
                                                       'Total_Rate_Total',
                                                       'ClaimAmount_Total',
                                                       'Total_Diff_Total',
                                                       'Label'])

        isp_blue_claim_all = isp_cost_final.select(['Baumuster_6',
                                                       'Product_Line',
                                                       'DamageCode',
                                                       'Total_Detail',
                                                       'ContractNumber',
                                                       'Signature_Date',
                                                       'Claimno',
                                                       'Product_EN',
                                                       'Claim_Amount',
                                                       'Claim_Date',
                                                       'Repair_Date',
                                                       'First_Registration_Date',
                                                       'VegaCredit_Date',
                                                       'Mileage_When_Repair',
                                                       'Repair_Age',
                                                       'Mileage_Per_Day',
                                                       'ServiceWorkshop',
                                                       'ServiceDealer_CN',
                                                       'ServiceGroup',
                                                       'ServiceRegion',
                                                       'ServiceProvince',
                                                       'ServiceCity',
                                                       'Workshop',
                                                       'Dealer_Name_CN',
                                                       'Owner_Group',
                                                       'Region',
                                                       'Province',
                                                       'City',
                                                       'Fin',
                                                       'Brand',
                                                       'Class',
                                                       'Model',
                                                       'Oil',
                                                       'Labor_Rate',
                                                       'Labor_Cost',
                                                       'Labor_Diff',
                                                       'Part_Total',
                                                       'Part_Cost',
                                                       'Part_Diff',
                                                       'Total_Rate',
                                                       'Total_Cost', 
                                                       'Total_Diff',
                                                       'Labor_Rate_Total',
                                                       'Labor_Cost_Total',
                                                       'Labor_Diff_Total',
                                                       'Part_Total_Total',
                                                       'Part_Cost_Total',
                                                       'Part_Diff_Total',
                                                       'Total_Rate_Total',
                                                       'ClaimAmount_Total',
                                                       'Total_Diff_Total',
                                                       'Label'])

        isp_claim_all = isp_blue_claim_all.unionAll(isp_silver_claim_all)

        isp_claim_all = isp_claim_all.select(['Baumuster_6',
                                       'Product_Line',
                                       'DamageCode',
                                       'Total_Detail',
                                       'ContractNumber',
                                       'Claimno',
                                       'Product_EN',
                                       'Claim_Amount',
                                       'Claim_Date',
                                       'Repair_Date',
                                       'First_Registration_Date',
                                       'Vegacredit_Date',
                                       'Mileage_When_Repair',
                                       'Repair_Age',
                                       'Mileage_Per_Day',
                                       'ServiceWorkshop',
                                       'ServiceDealer_CN',
                                       'ServiceGroup',
                                       'ServiceRegion',
                                       'ServiceProvince',
                                       'ServiceCity',
                                       'Workshop',
                                       'Dealer_Name_CN',
                                       'Owner_Group',
                                       'Region',
                                       'Province',
                                       'City',
                                       'Fin',
                                       'Brand',
                                       'Class',
                                       'Model',
                                       'Oil',
                                       'Labor_Rate',
                                       'Labor_Cost',
                                       'Labor_Diff',
                                       'Part_Total',
                                       'Part_Cost',
                                       'Part_Diff',
                                       'Total_Rate',
                                       'Total_Cost', 
                                       'Total_Diff',
                                       'Labor_Rate_Total',
                                       'Labor_Cost_Total',
                                       'Labor_Diff_Total',
                                       'Part_Total_Total',
                                       'Part_Cost_Total',
                                       'Part_Diff_Total',
                                       'Total_Rate_Total',
                                       'ClaimAmount_Total',
                                       'Total_Diff_Total',
                                       'Label',
                                       'Signature_Date'])

        w =  Window.partitionBy(isp_claim_all.ContractNumber,isp_claim_all.DamageCode).orderBy(isp_claim_all.Repair_Date)                                   
        isp_claim_all = isp_claim_all.withColumn('Damage_Sort',dense_rank().over(w))
        isp_claim_all = isp_claim_all.join(contract.select(['ContractNumber','Activation_date']),'ContractNumber','left')

        return isp_claim_all

        

    """
    cost control of claim for total service
    for example:
        1A+1B refers to a contract
        Each contract has its own upperlimit for cost of claim
        Normally, dealers need to claim costs below upperlimit, 
        but some will claim more which requires more attention to be controlled                 
    """
        
    def _get_contract_control(self,blue_contract_esse,blue_cost_esse):
        
        rename_string_text = """baumuster_6, 
        service_type as product_line, 
        value_a, 
        value_b, 
        value_sp, 
        value_ac"""
        
                
        price_blue = self.sc.sql("select " + rename_string_text + \
        " from revr_bmbs_srv_contract_blue.src_price_blue_info")

        price_blue_amg = self.sc.sql("select " + rename_string_text + \
        " from revr_bmbs_srv_contract_blue.src_price_blue_amg_info")
        
        price_bluelight = self.sc.sql("select " + rename_string_text + \
        " from revr_bmbs_srv_contract_blue.src_price_bluelight_info")
        
        price_bluelight_amg = self.sc.sql("select " + rename_string_text + \
        " from revr_bmbs_srv_contract_blue.src_price_bluelight_amg_info")   
        
        cols = StructType([StructField('Baumuster_6', StringType(),True),
            StructField('Product_Line', StringType(),True),
            StructField('Value_A', StringType(),True),           
            StructField('Value_B', StringType(),True),
            StructField('Value_SP', StringType(),True),
            StructField('Value_AC', StringType(),True)])
                
        price_blue = spark.createDataFrame(price_blue.rdd,cols)
        price_blue_amg = spark.createDataFrame(price_blue_amg.rdd,cols)
        price_bluelight = spark.createDataFrame(price_bluelight.rdd,cols)
        price_bluelight_amg = spark.createDataFrame(price_bluelight_amg.rdd,cols)

        blue_contract_esse = blue_contract_esse.withColumnRenamed('Service_Type','Product_Line')
                
        blue_claim = blue_contract_esse.filter((blue_contract_esse.Product_EN == 'GMP') & (blue_contract_esse.Oil == 'Normal'))
        blue_amg_claim = blue_contract_esse.filter((blue_contract_esse.Product_EN == 'GMP') & (blue_contract_esse.Oil == 'AMG'))
        
        bluelight_claim = blue_contract_esse.filter((blue_contract_esse.Product_EN == 'GMP light') & (blue_contract_esse.Oil == 'Normal'))
        bluelight_amg_claim = blue_contract_esse.filter((blue_contract_esse.Product_EN == 'GMP light') & (blue_contract_esse.Oil == 'AMG'))

        blue_claim= blue_claim.join(price_blue, on = ['Baumuster_6', 'Product_Line'], how = 'left')
        blue_amg_claim= blue_amg_claim.join(price_blue_amg, on = ['Baumuster_6', 'Product_Line'], how = 'left')
        bluelight_claim= bluelight_claim.join(price_bluelight, on = ['Baumuster_6', 'Product_Line'], how = 'left')
        bluelight_amg_claim= bluelight_amg_claim.join(price_bluelight_amg, on = ['Baumuster_6', 'Product_Line'], how = 'left')

        blue_contract_all  = union(blue_claim,blue_amg_claim,bluelight_claim,bluelight_amg_claim)
        
        #calculate upperlimit column
        blue_contract_all= blue_contract_all.withColumn('TotalUpperLimit',(blue_contract_all.A_Count*blue_contract_all.Value_A+\
                                                                           blue_contract_all.Value_B*blue_contract_all.B_Count+\
                                                                           blue_contract_all.A_CFilter_Count*blue_contract_all.Value_AC+\
                                                                           blue_contract_all.Spark_Plug_Count*blue_contract_all.Value_SP))
        
        
        othercost = blue_cost_esse.filter(blue_cost_esse.DamageCode=='2103601')\
                                         .groupby('ContractNumber')\
                                                 .agg({'Claim_Amount':'sum'})
                                                 
        othercost = othercost.withColumnRenamed('sum(Claim_Amount)','OtherCost')
        
        blue_contract_all = blue_contract_all.join(othercost, 'ContractNumber','left')
        blue_contract_all = blue_contract_all.na.fill({'OtherCost':0})
        
        blue_gap = blue_contract_all.TotalUpperLimit - blue_contract_all.CostTillNow + blue_contract_all.OtherCost
        blue_contract_all = blue_contract_all.withColumn('Gap', (blue_gap))
        
        #identify exceedamount interval
        func_get_excamount = udf(get_excamount, StringType())
        blue_contract_all = blue_contract_all.withColumn('ExceedAmount',func_get_excamount('Gap'))
        
        lastrepairdate = blue_cost_esse.select(['ContractNumber','Claimno','Repair_Date'])
        
        #select the newest repair date
        windowSpec = Window.partitionBy(lastrepairdate['ContractNumber']).orderBy(lastrepairdate['Repair_Date'].desc())
        lastrepairdate = lastrepairdate.select('ContractNumber', 'Claimno', 'Repair_Date', dense_rank().over(windowSpec).alias('SortOrder'))
        lastrepairdate = lastrepairdate.filter(lastrepairdate.SortOrder == 1)
        
        #one contract may need to be repaired twice in one day
        #drop duplicated records based on this situation
        lastrepairdate = lastrepairdate.drop_duplicates(['ContractNumber'])
        
        #blue_contract merge lastrepairdate
        blue_contract_all = blue_contract_all.join(lastrepairdate.select(['ContractNumber','Repair_Date']),'ContractNumber','left')
        
        print("Load data in table tmp_blue_contract_upperlimit_info")
        blue_contract_all.registerTempTable("blue_upperlimit")
        self.sc.sql("truncate table revr_bmbs_srv_contract_blue.tmp_blue_contract_upperlimit_info")
        self.sc.sql("insert into table revr_bmbs_srv_contract_blue.tmp_blue_contract_upperlimit_info select * from blue_upperlimit")        
        
        return blue_contract_all
        

    def _get_isp_contract_control(self,isp_blue_contract_esse,isp_blue_cost_esse,isp_gap_price):
      
        isp_contract_blue_all=isp_blue_contract_esse.join(isp_gap_price,'Baumuster_6','left')

        isp_cont_fill_vc_na_list = ['Value_A','Value_AMG_A','Value_B','Value_AMG_B','Value_AC_filter','Value_SparkPlug','Value_RBDO','Value_FBFP','Value_BF','Value_FWB','Value_RWB']
        isp_cont_fill_vc_na_dict = {i:-10 for i in isp_cont_fill_vc_na_list}
        isp_contract_blue_all = isp_contract_blue_all.na.fill(isp_cont_fill_vc_na_dict)
        

        func_damage_code_na_union = udf(damage_code_na_union, StringType())
        isp_contract_blue_all = isp_contract_blue_all.withColumn('Union_Damage',func_damage_code_na_union('Value_A','Value_AMG_A','Value_B','Value_AMG_B','Value_AC_filter','Value_SparkPlug','Value_RBDO','Value_FBFP','Value_BF','Value_FWB','Value_RWB'))
        isp_filter_damage=isp_contract_blue_all.select('ContractNumber','Union_Damage')
        isp_filter_damage=isp_filter_damage.join(isp_blue_cost_essence.select(['ContractNumber','Damagecode','Claim_Amount']),'ContractNumber','left')        
        func_filter_damage_code = udf(filter_damage_code, StringType())
        isp_filter_damage=isp_filter_damage.withColumn('Damage_Status',func_filter_damage_code('Union_Damage','Damagecode'))
        isp_filter_damage = isp_filter_damage.filter(isp_filter_damage.Damage_Status=='Not Available')  
        isp_filter_damage=isp_filter_damage.groupby('ContractNumber').agg({'Claim_Amount':'sum'}).withColumnRenamed('sum(Claim_Amount)','Unavailable_Cost')
        isp_contract_blue_all=isp_contract_blue_all.join(isp_filter_damage,'ContractNumber','left')
        
        isp_contract_blue_all = isp_contract_blue_all.na.fill({'Unavailable_Cost':0})
             
        isp_othercost = isp_blue_cost_esse.filter(isp_blue_cost_esse.DamageCode=='2103601').groupby('ContractNumber').agg({'Claim_Amount':'sum'})
                                                 
        isp_othercost = isp_othercost.withColumnRenamed('sum(Claim_Amount)','OtherCost')
        isp_contract_blue_all = isp_contract_blue_all.join(isp_othercost, 'ContractNumber','left')
        isp_contract_blue_all = isp_contract_blue_all.na.fill({'OtherCost':0})
        
        func_total_upperlimit_cal = udf(total_upperlimit_cal, StringType())
        isp_contract_blue_all=isp_contract_blue_all.withColumn('TotalUpperLimit',func_total_upperlimit_cal('Value_A','Value_AMG_A','Value_B','Value_AMG_B','Value_AC_filter','Value_SparkPlug','Value_RBDO','Value_FBFP','Value_BF','Value_FWB','Value_RWB'))

        #exclude unvailible cost that the price info is null
        isp_blue_gap = isp_contract_blue_all.TotalUpperLimit - isp_contract_blue_all.CostTillNow + isp_contract_blue_all.OtherCost + isp_contract_blue_all.Unavailable_Cost
        isp_contract_blue_all = isp_contract_blue_all.withColumn('Gap', (isp_blue_gap))
  
        #identify exceedamount interval
        func_get_excamount = udf(get_excamount, StringType())
        isp_contract_blue_all = isp_contract_blue_all.withColumn('ExceedAmount',func_get_excamount('Gap'))
        
        lastrepairdate = isp_blue_cost_esse.select(['ContractNumber','Claimno','Repair_Date'])
        
        #select the newest repair date
        windowSpec = Window.partitionBy(lastrepairdate['ContractNumber']).orderBy(lastrepairdate['Repair_Date'].desc())
        lastrepairdate = lastrepairdate.select('ContractNumber', 'Claimno', 'Repair_Date', dense_rank().over(windowSpec).alias('SortOrder'))
        lastrepairdate = lastrepairdate.filter(lastrepairdate.SortOrder == 1)
        
        #one contract may need to be repaired twice in one day
        #drop duplicated records based on this situation
        lastrepairdate = lastrepairdate.drop_duplicates(['ContractNumber'])
        
        #blue_contract merge lastrepairdate
        isp_contract_blue_all = isp_contract_blue_all.join(lastrepairdate.select(['ContractNumber','Repair_Date']),'ContractNumber','left')
        
        isp_contract_blue_all = isp_contract_blue_all.withColumnRenamed('A/Cfilter_count','ACfilter_count').withColumnRenamed('A/C','AC').withColumnRenamed('detail_A/C','detail_AC')
        
#        print("Load data in table tmp_blue_contract_upperlimit_info")
#        isp_contract_blue_all.registerTempTable("isp_blue_upperlimit")
#        self.sc.sql("truncate table revr_bmbs_srv_contract_blue.tmp_isp_blue_contract_upperlimit_info")
#        self.sc.sql("insert into table revr_bmbs_srv_contract_blue.tmp_isp_blue_contract_upperlimit_info select * from isp_blue_upperlimit")        
#        
        return isp_contract_blue_all
 
































