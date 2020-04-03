#!/bin/bash -e
#####################################################################################
#                    Created By  :  Manish Pansari                                  #
#                    Created On  :  03-30-2020                                      #
#                    Created For :  Salesforce Bouns Calculation                    #
#                    Modified On :  04-02-2020                                      #
####################################################################################

LOG_TS=$(date '+%Y-%m-%dT%H%M')
start_timestamp=$(date --date="today" +"%Y-%m-%d_%H%M%S")
HOME_DIR="/home/025d3777f4b1ecm/BonusCalculation/Salesforce"
LOG_DIR="$HOME_DIR/log"
LOG_PATH="$LOG_DIR""/bonus_salesforce_spark$LOG_TS"".log"
emailList="mpansari@godaddy.com"

mkdir -p "$LOG_DIR"
if /opt/spark/2.3/bin/spark-class org.apache.spark.deploy.SparkSubmit --deploy-mode client --jars 'hdfs:///user/hvalluri/sqljdbc42.jar' --py-files $HOME_DIR/properties.py $HOME_DIR/SalesforceBonusCalculation.py >> "$LOG_PATH" 2>&1; then
	end_timestamp=$(date --date="today" +"%Y-%m-%d_%H%M%S")
	echo "Spark job Execution started at $start_timestamp"|tee -a "$LOG_PATH"
	echo "Spark job Execution completed at $end_timestamp"|tee -a "$LOG_PATH"
else
	echo "BonusCalculation Salesforce Spark job Execution Failed at $LOG_TS"|tee -a  "$LOG_PATH"
	echo "BonusCalculation Salesforce Spark job Execution Failed at $LOG_TS. Please check the airflow log"| mail -s "BonusCalculation Salesforce Spark Job Failure Alert" $emailList
	exit 1;
fi
