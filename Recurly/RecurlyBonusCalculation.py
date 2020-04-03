############################################################################################
## Business Area : Bouns Calculation (Using Salesforce and Recurly data)                ####
## Date Changed  : 03-26-2020                                                           ####
## Report Name   : Bouns Calculation                                                    ####
## Written By.   : Manish Pansari                                                       ####
############################################################################################                                

#ipyspark36-2.3 --executor-memory 6g --executor-cores 4 --driver-memory 8g --conf spark.yarn.executor.memoryOverhead=4096 --conf spark.buffer.pageSize=2m --conf spark.sql.shuffle.partitions=2000 --conf spark.maxremoteblocksizefetchtomem=1.5g --conf spark.memory.fraction=0.7 --conf spark.memory.storageFraction=0.3
#--py-files {0}/properties.py {0}/

#HOME_DIR = "/home/025d3777f4b1ecm/pds_care_contact_spark"
#spark_command = "/opt/spark/2.3/bin/spark-class org.apache.spark.deploy.SparkSubmit --num-executors 10 --executor-memory 3g --driver-memory 3g --executor-cores 3 --deploy-mode client  --py-files {0}/properties.py {0}/".format(HOME_DIR)


#Initialize 
from pyspark.sql import SparkSession
from properties import properties
import pyspark
print(pyspark.__version__)

try:
    if __name__ == "__main__":
        spark = SparkSession \
                .builder \
                .appName("Recurly Bonus Calculation") \
                .enableHiveSupport() \
                .getOrCreate()
        
        #Creating final bonus table
        recurly_bonus_df = spark.sql("""
                                    SELECT DISTINCT loc.location_id AS locationId, 
                                           loc.business_name AS businessName, 
                                           --From_unixtime(loc.creation_sec)     AS start_date, 
                                           From_unixtime(loc.cancellation_sec) AS cancelDate, 
                                           CASE 
                                             WHEN ecomm.ecommerce_mrr IS NULL 
                                                  AND lp.price < 5 THEN Nvl(adp.addon_price, 0) + Nvl(pdl.price, 0) 
                                             WHEN ecomm.ecommerce_mrr IS NULL 
                                                  AND lp.price >= 5 THEN Nvl(adp.addon_price, 0) + Nvl(lp.price, 0) 
                                             WHEN ecomm.ecommerce_mrr > 0 THEN ecomm.ecommerce_mrr 
                                           END                                 AS currentPrice 
                                    FROM   msh.location loc 
                                           LEFT JOIN msh.location_plan lp 
                                                  ON loc.location_id = lp.location_id 
                                           LEFT JOIN (SELECT addon.location_id, 
                                                             addon.facebook_post_addon_price 
                                                             + addon.hubmail_addon_price 
                                                             + addon.hubsite_addon_price 
                                                             + addon.hubtargeter_addon_price 
                                                             + addon.tripadvisor_response_addon_price 
                                                             + addon.twitter_post_addon_price AS addon_price 
                                                      FROM   msh.addon_price_history AS addon 
                                                      WHERE  addon.to_sec IS NULL 
                                                             AND ( addon.facebook_post_addon_price 
                                                                   + addon.hubmail_addon_price 
                                                                   + addon.hubsite_addon_price 
                                                                   + addon.hubtargeter_addon_price 
                                                                   + addon.tripadvisor_response_addon_price ) > 0) 
                                                     adp 
                                                  ON loc.location_id = adp.location_id 
                                           LEFT JOIN msh.salesforce_account act 
                                                  ON loc.location_id = act.location_id 
                                           LEFT JOIN (SELECT est1.shopper_id, 
                                                             est1.product_name, 
                                                             est1.product_period_name, 
                                                             est1.gcr_amt, 
                                                             est1.pf_id, 
                                                             est1.product_month_qty, 
                                                             est1.gcr_amt / est1.product_month_qty AS ecommerce_MRR 
                                                      FROM   madams.ecomm_sales_test AS est1 
                                                             INNER JOIN (SELECT shopper_id, 
                                                                                Max(order_date) AS MaxDate 
                                                                         FROM   madams.ecomm_sales_test 
                                                                         GROUP  BY shopper_id) estmax 
                                                                     ON est1.shopper_id = estmax.shopper_id 
                                                                        AND est1.order_date = estmax.maxdate) AS 
                                                     ecomm 
                                                  ON ecomm.shopper_id = act.gddy_shopper_id 
                                           LEFT JOIN madams.plan_defaults_lookup pdl 
                                                  ON lp.plan_name = pdl.display_name 
                                    WHERE  ( From_unixtime(loc.cancellation_sec) IS NULL 
                                              OR From_unixtime(loc.cancellation_sec) >= 
                                                 Date_format(CURRENT_DATE, 'yyyy-MM-01') ) 
                                           AND From_unixtime(loc.creation_sec) < 
                                               Date_format(CURRENT_DATE, 'yyyy-MM-01')
                                    """)
        recurly_bonus_df = recurly_bonus_df.select("locationId", "businessName", "cancelDate", "currentPrice")

        # Need to delete the below line, once we get confirmation.
        recurly_bonus_df1 = recurly_bonus_df.where("locationId not in (59311,59307)")

        ##set variable to be used to connect the database
        database = "CallCenterReporting"
        table = "msh.bonusRecurlyData_Staging"
        # user = "do-as-datamgmt"
        # password  = "******"

        # write spark dataframe into sql server
        recurly_bonus_df1.write.mode("overwrite") \
                        .option("truncate", True) \
                        .option("numPartitions", properties.maxDBConnections) \
                        .format("jdbc") \
                        .option("url",f"jdbc:sqlserver://P3PWBIGRPTSQL02.dc1.corp.gd:1433;databaseName=CallCenterReporting;",properties=properties.dbProperties) \
                        .option("dbtable", table) \
                        .save()
        print("Data successfully loaded into " + str(table) + "....")
        spark.stop()


except Exception as e:
    print("Exception occured")
    print("Error message is  ::" +str(e))
    raise e