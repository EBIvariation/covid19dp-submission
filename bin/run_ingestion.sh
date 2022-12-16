#!/bin/bash

#email_recipient=***********
#eva_dir=**************
# Grab the variables from a config bash script
source ~/.covid19dp_processing

if [ -z "${email_recipient}" ] || [ -z "${eva_dir}" ];
then
  echo "run_ingestion.sh does not have access to email_recipient and eva_dir variables. Please set them above or populate ~/.covid19dp_processing"
  exit 1
fi

tmp_dir=${eva_dir}/scratch
project=PRJEB45554
taxonomy=2697049
software_dir=${eva_dir}/software/covid19dp-submission/production_deployments/
project_dir=${eva_dir}/data/${project}
lock_file=${project_dir}/.lock_ingest_covid19dp_submission
number_to_process=10000


#Check if the previous process is still running
if [[ -e ${lock_file} ]];
then
  echo "processing in $(cat ${lock_file}) is still going. Exit"
  exit 0
fi

# Check if there is an unfinished process
valid_dir=${project_dir}/30_eva_valid

for dir in ${valid_dir}/????_??_??_??_??_??;
do
  if [[ ! -e ${dir}/.process_complete ]];
  then
    current_date=$(basename ${dir})
    break
  fi
done

# If the current_date is set and if it isn't then create a new one
if [ -z "$current_date" ];
then
  current_date=$(date --rfc-3339=second | cut -d '+' -f 1 | sed 's/[- :]/_/g' )
fi

processing_dir=${valid_dir}/${current_date}
log_dir=${project_dir}/00_logs/${current_date}
public_dir=${project_dir}/60_eva_public/${current_date}

echo ${current_date} > ${lock_file}
# Ensure that the lock file is delete on exit of the script
trap 'rm ${lock_file}' EXIT

# Need to create the directories because we're using resume from the first execution so they won't be created by
# ingest_covid19dp_submission.py
mkdir -p ${processing_dir} ${log_dir} ${public_dir}
export TMPDIR=${tmp_dir}

${software_dir}/production/bin/ingest_covid19dp_submission.py \
  --project ${project} --accepted-taxonomies ${taxonomy} \
  --project-dir ${project_dir} --app-config-file ${software_dir}/app_config.yml \
  --nextflow-config-file ${software_dir}/workflow.config \
  --processed-analyses-file ${project_dir}/processed_analysis.txt \
  --ignored-analyses-file ${project_dir}/ignored_analysis.txt \
  --num-analyses ${number_to_process} \
  --resume-snapshot ${current_date} \
  >> ${processing_dir}/ingest_covid19dp.log \
  2>> ${processing_dir}/ingest_covid19dp.err

# Assess success
process_exit_status=$?
set -o pipefail && cat ${processing_dir}/ingest_covid19dp.log | grep 'submission_workflow.nf' | grep 'completed successfully'
grep_exit_status=$?
if [ ${process_exit_status} == 0 ] && [ ${grep_exit_status} == 0 ];
then
  touch ${processing_dir}/.process_complete
  nb_processed=$(cat ${project_dir}/processed_analysis.txt| wc -l)
  cat > ${processing_dir}/email <<- EOF
From: eva-noreply@ebi.ac.uk
To: ${email_recipient}
Subject: COVID19 Data Processing batch ${current_date} completed successfully

Accessioning/Clustering of ${number_to_process} new COVID19 samples started in ${current_date} is now complete.
The total number of samples processed is ${nb_processed}
EOF
  cat ${processing_dir}/email | sendmail ${email_recipient}
else
  cat > ${processing_dir}/email <<- EOF
From: eva-noreply@ebi.ac.uk
To: ${email_recipient}
Subject: COVID19 Data Processing batch ${current_date} failed

Accessioning/Clustering processing batch ${current_date} failed.
EOF
  cat ${processing_dir}/email | sendmail ${email_recipient}
fi