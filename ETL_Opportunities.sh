#!/bin/bash

#Initialize
objectname="Opportunity"
approot="/home/ec2-user/salesforce_pull"
queryfile="$approot/bin/download_opportunity.sql"
heroku_postgres_jdbc="postgres://{user}:{password}@{host}}:5432/databasename"
trnsactionid=`date +'%Y%m%d%H%M%S'`
outputdir_raw="$approot/raw/$trnsactionid"
outputdir_conf="$approot/conformed/$trnsactionid"
mkdir -p $outputdir_raw
mkdir -p $outputdir_conf
outputfilename_csv="$objectname.csv"
outputfilename_pq="$objectname.parquet"
localoutputfile_raw_csv="$outputdir_raw/$outputfilename_csv"
localoutputfile_raw_pq="$outputdir_raw/$outputfilename_pq"
localoutputfile_conf_pq="$outputdir_conf/$outputfilename_pq"
s3outputfile_raw_pq="s3://as-datalake-raw-staging/salesforce/landed-from-heroku/$trnsactionid/$outputfilename_pq"
s3outputfile_conf_pq="s3://as-datalake-conformed-staging/salesforce/conformed/$trnsactionid/$outputfilename_pq"
manifest_raw_local="$outputdir_raw/raw.m"
manifest_conf_local="$outputdir_conf/conformed.m"
s3manifestfile_raw="s3://as-datalake-raw-staging/salesforce/landed-from-heroku/$trnsactionid/raw.m"
s3manifestfile_conf="s3://as-datalake-conformed-staging/salesforce/conformed/$trnsactionid/conformed.m"



###########
# Extract #
###########

#Start download
echo ">> $objectname pull started.."
psql $heroku_postgres_jdbc -A -F"," -f $queryfile -o $localoutputfile_raw_csv
echo ">> $objectname download to local completed."

#Code here for csv to parquet transformation and save with name as per variable outputfilename_pq
#.....
#below is a dummy logic as a placeholder
cp $localoutputfile_raw_csv $localoutputfile_raw_pq

#Upload object data to S3
aws s3 cp $localoutputfile_raw_pq $s3outputfile_raw_pq
echo ">> Uploaded $objectname data to s3 raw staging in parquet format"

#Upload manifest file to s3
echo "datafile=$s3outputfile_raw_pq" > $manifest_raw_local
echo "transactionid=$trnsactionid" >> $manifest_raw_local
aws s3 cp $manifest_raw_local $s3manifestfile_raw
echo ">> Uploaded manifest file to s3 raw staging"

echo ">> Upload to s3 raw staging completed"

###############
# Conformance #
###############

#-- dummy code here for conformance and drop the final Opportunity.parquet in conformed/$transactionid/ folder
#-- placeholder logic. just copy to conformed local
echo ">> Conformance process started..."
cp $localoutputfile_raw_pq $localoutputfile_conf_pq

#Upload conformed data to conformed S3 location
echo ">> Uploadeing conformed data to S3..."
aws s3 cp $localoutputfile_conf_pq $s3outputfile_conf_pq

#Create conformed manifest file
echo ">> Uploadeing conformed manifest file to S3..."
echo "datafile=$s3outputfile_conf_pq" > $manifest_conf_local
echo "transactionid=$trnsactionid" >> $manifest_conf_local
aws s3 cp $manifest_conf_local $s3manifestfile_conf

sleep 10

########
# Done #
########
echo ">> All complete."

echo ">> Shutting down EC2 instance.."

aws lambda invoke --function-name StopEC2Lamda response
