#PWD=`pwd`
#
#input="file://${PWD}/local_test_data/sitemap_spider_output.txt"
#output="file://${PWD}/local_test_data/output"
#SITEMAP_TYPE="web"
#
#rm -rf "${PWD}/local_test_data/output"
#${SPARK_HOME}/bin/spark-submit \
#  --master local[2] \
#  bin/get_parsed_num.py \
#  $input \
#  $output \
#  ${SITEMAP_TYPE}


INPUT=$1
OUTPUT=$2
SITEMAP_TYPE=$3

hadoop fs -rmr ${OUTPUT}
${SPARK_HOME}/bin/spark-submit \
  --name "sitemap.stat_sitemap_parsed_num" \
  --queue "root.spider.refresh" \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 200 \
  --executor-cores 2 \
  --executor-memory 2G \
  --driver-memory 2G \
  bin/get_parsed_num.py \
  ${INPUT} \
  ${OUTPUT} \
  ${SITEMAP_TYPE}
