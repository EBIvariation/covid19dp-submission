#!/bin/bash

#email_recipient=***********
# Grab the variables from a config bash script
source ~/.covid19dp_processing

if [ -z "${eva_dir}" ] ;
then
  echo "run_ingestion.sh does not have access to eva_dir variable. Please set it above or populate ~/.covid19dp_processing"
  exit 1
fi
project=$1
software_dir=${eva_dir}/software/covid19dp-submission/production_deployments/
project_dir=${eva_dir}/data/${project}
lock_file=${project_dir}/.lock_ingest_covid19dp_submission

#Check if the previous process is still running
if [[ -e ${lock_file} ]];
then
  echo "processing in $(cat ${lock_file}) is still going. Exit"
  exit 0
fi

# Also check if there isn't a ingest_covid19dp process that would not started
if bjobs -o 'job_name:100' | grep 'ingest_covid19dp' > /dev/null;
then
  echo "Lock file is not set but there is an ingest_covid19dp job running"
else
  bsub -e ${project_dir}/run_ingestion.err -o ${project_dir}/run_ingestion.out -J ingest_covid19dp -M 20G \
   -R "rusage[mem=20960]" "${software_dir}/production/bin/run_ingestion.sh ${project}"
fi
