############################################################################################
## Business Area : Bouns Calculation (Using Salesforce)                					####
## Date Changed  : 03-26-2020                                                           ####
## Report Name   : Salesforce Bouns Calculation                                         ####
## Written By.   : Manish Pansari                                                       ####
############################################################################################                                

#ipyspark36-2.3 --executor-memory 6g --executor-cores 4 --driver-memory 8g --conf spark.yarn.executor.memoryOverhead=4096 --conf spark.buffer.pageSize=2m --conf spark.sql.shuffle.partitions=2000 --conf spark.maxremoteblocksizefetchtomem=1.5g --conf spark.memory.fraction=0.7 --conf spark.memory.storageFraction=0.3
#HOME_DIR = "/home/025d3777f4b1ecm/pds_care_contact_spark"
#spark_command = "/opt/spark/2.3/bin/spark-class org.apache.spark.deploy.SparkSubmit --num-executors 10 --executor-memory 3g --driver-memory 3g --executor-cores 3 --deploy-mode client  --py-files {0}/properties.py {0}/".format(HOME_DIR)


#Initialize 
from pyspark.sql import SparkSession
import pyspark
print(pyspark.__version__)

try:
    if __name__ == "__main__":
        spark = SparkSession \
        .builder \
        .appName("Salesforce Bonus Calculation") \
        .enableHiveSupport() \
        .getOrCreate()

        # Creating final bonus table
        salesforce_bonus_df = spark.sql("""	SELECT 
														DISTINCT opp.id AS opportunityId, 
														account.start_date_cst AS startDate, 
														opp.close_date_cst AS closeDate, 
														opp.amount AS price, 
														opp.product AS productName, 
														opp.type AS opportunityRecordType, 
														account.id AS accountId, 
														account.msh_industry AS industry,
														usr_hist.accounting_id AS adpFileNumber,
														account.location_id AS locationNumber,
														account.is_deal_reversal AS dealReversal, 
														usr_hist.channel AS channel
													FROM msh.salesforce_opportunity opp 
													LEFT JOIN msh.salesforce_lead lead 
														   ON lead.opportunity_id = opp.id
													LEFT JOIN msh.salesforce_account account 
													       ON opp.account_id = account.id
													LEFT JOIN ( SELECT * 
																FROM msh.salesforce_user_history 
																WHERE most_recent_user_record = TRUE
															  ) usr_hist 
													       ON usr_hist.user_id = opp.owner_id
												""")

        salesforce_bonus_df = salesforce_bonus_df.select("opportunityId",
                                                         "accountId",
                                                         "price",
                                                         "productName",
                                                         "opportunityRecordType",
                                                         "industry",
                                                         "adpFileNumber",
                                                         "locationNumber",
                                                         "dealReversal",
                                                         "channel",
                                                         "startDate",
                                                         "closeDate"
                                                         )

        # #set variable to be used to connect the database
        database = "CallCenterReporting"
        table = "msh.bonusSalesForceData_Staging"
        user = "do-as-datamgmt"
        password  = "******"

        # write spark dataframe into sql server
        salesforce_bonus_df.write.mode("overwrite") \
                            .format("jdbc") \
                            .option("url", f"jdbc:sqlserver://P3PWBIGRPTSQL02.dc1.corp.gd:1433;databaseName=CallCenterReporting;") \
                            .option("dbtable", table) \
                            .option("user", user) \
                            .option("password", password) \
                            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                            .save()
		spark.stop()
        

except Exception as e:
    print("Exception occured")
    print("Error message is  ::" +str(e))
    raise e