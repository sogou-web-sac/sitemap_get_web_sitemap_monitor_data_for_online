#PWD=`pwd`
#
#input1="file://${PWD}/local_test_data/sitemap_spider_output.txt"
#input2="file://${PWD}/local_test_data/fetched_urls.txt"
#output="file://${PWD}/local_test_data/output"
#SITEMAP_TYPE="web"
#
#rm -rf "${PWD}/local_test_data/output"
#${SPARK_HOME}/bin/spark-submit \
#  --master local[2] \
#  bin/get_fetched_num.py \
#  $input1 \
#  $input2 \
#  $output \
#  ${SITEMAP_TYPE}

INPUT1=$1
INPUT2=$2
OUTPUT=$3
SITEMAP_TYPE=$4

hadoop fs -rmr ${OUTPUT}
${SPARK_HOME}/bin/spark-submit \
  --name "sitemap.stat_sitemap_fetched_num" \
  --queue "root.spider.refresh" \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 200 \
  --executor-cores 2 \
  --executor-memory 2G \
  --driver-memory 2G \
  bin/get_fetched_num.py \
  ${INPUT1} \
  ${INPUT2} \
  ${OUTPUT} \
  ${SITEMAP_TYPE}
