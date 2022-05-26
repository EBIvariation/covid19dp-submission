#!/bin/bash

basedir=$(dirname $(dirname $(readlink -f $0)))
echo eva_dir=$basedir/tests/resources > ~/.covid19dp_processing
export PATH=$basedir/tests/resources/bin:$PATH
prj_dir=$basedir/tests/resources/data/PRJEB45554


function tearDown {
  rm -rf $basedir/tests/resources/data
  rm ~/.covid19dp_processing
}
trap tearDown EXIT


if ${basedir}/bin/run_ingestion_cron.sh | grep '<submitted>' > /dev/null  ;
then
  echo 'Test 1 Pass'
else
  echo 'Test 1 Fail'
  exit 1
fi
mkdir -p $prj_dir
touch $prj_dir/.lock_ingest_covid19dp_submission

if ${basedir}/bin/run_ingestion_cron.sh | grep '<submitted>' > /dev/null  ;
then
  echo 'Test 2 Fail'
  exit 1
else
  echo 'Test 2 Pass'
fi
