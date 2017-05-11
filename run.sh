YYYY=`date +%Y -d "2 day ago"`
mm=`date +%m -d "2 day ago"`
dd=`date +%d -d "2 day ago"`
TIME=`date +%Y%m%d -d "2 day ago"`

MASTER="-1"
for i in `seq 1 8`
do
  hadoop fs -test -e hftp://master00$i.diablo.hadoop.nm.ted:50070/user/zhangbei/sitemap/CountSitemapHostParseNum/
  if [ "$?" = "0" ]; then
    MASTER=$i
    break
  fi
done

echo "MASTER = hftp://master00${MASTER}.diablo.hadoop.nm.ted:50070"

INPUT0="hftp://master01.zeus.hadoop.ctc.sogou-op.org:50070/storage/sogou/web/new_sitemap/sitemap_pc/${YYYY}/${mm}/${dd}/*/*"
INPUT2="hftp://master003.diablo.hadoop.nm.ted:50070/user/zhangbei/sitemap/newurl_seeds/${YYYY}/${YYYY}${mm}/${TIME}/*"
INPUT3="hftp://master003.diablo.hadoop.nm.ted:50070/user/zhangbei/sitemap/CountSitemapFetchResult/${YYYY}/${YYYY}${mm}/${TIME}/*"
INPUT4="hftp://master003.diablo.hadoop.nm.ted:50070/user/zhangbei/sitemap/SingleSitemapAnalyse/${YYYY}/${YYYY}${mm}/${TIME}/*/indb-r-*"

#INPUT0="hftp://master01.zeus.hadoop.ctc.sogou-op.org:50070/storage/sogou/web/new_sitemap/sitemap_pc/2017/05/09/10/instspider08.web.zw.vm.ted_2017-05-09_10"
#INPUT2="hftp://master003.diablo.hadoop.nm.ted:50070/user/zhangbei/sitemap/newurl_seeds/2017/201705/20170509/part-r-00000"
#INPUT3="hftp://master003.diablo.hadoop.nm.ted:50070/user/zhangbei/sitemap/CountSitemapFetchResult/2017/201705/20170509/part-r-00000"
#INPUT4="hftp://master003.diablo.hadoop.nm.ted:50070/user/zhangbei/sitemap/SingleSitemapAnalyse/2017/201705/20170509/*/indb-r-007*"

PARSED_DIR="sitemap_parsed_num"
SELECTED_DIR="sitemap_selected_num"
FETCHED_DIR="sitemap_fetched_num"
IN_NORM_DIR="sitemap_in_norm_num"
OUTPUT1="/user/zhufangze/sitemap/online_monitor_data/web/${PARSED_DIR}/${TIME}"
OUTPUT2="/user/zhufangze/sitemap/online_monitor_data/web/${SELECTED_DIR}/${TIME}"
OUTPUT3="/user/zhufangze/sitemap/online_monitor_data/web/${FETCHED_DIR}/${TIME}"
OUTPUT4="/user/zhufangze/sitemap/online_monitor_data/web/${IN_NORM_DIR}/${TIME}"

hadoop fs -rmr ${OUTPUT1}
hadoop fs -rmr ${OUTPUT2}
hadoop fs -rmr ${OUTPUT3}
hadoop fs -rmr ${OUTPUT4}

SITEMAP_TYPE="web"
sh run_get_parsed_num.sh ${INPUT0} ${OUTPUT1} ${SITEMAP_TYPE} 1>log/std_${TIME}.log 2>log/err_${TIME}.log 
sh run_get_selected_num.sh ${INPUT0} ${INPUT2} ${OUTPUT2} ${SITEMAP_TYPE} 1>log/std_${TIME}.log 2>log/err_${TIME}.log
sh run_get_fetched_num.sh ${INPUT0} ${INPUT3} ${OUTPUT3} ${SITEMAP_TYPE} 1>log/std_${TIME}.log 2>log/err_${TIME}.log
sh run_get_innorm_num.sh ${INPUT4} ${OUTPUT4} 1>log/std_${TIME}.log 2>log/err_${TIME}.log


rm -rf data/${TIME}
mkdir data/${TIME}
hadoop fs -text ${OUTPUT1}/* > data/${TIME}/${PARSED_DIR}.txt 
hadoop fs -text ${OUTPUT2}/* > data/${TIME}/${SELECTED_DIR}.txt 
hadoop fs -text ${OUTPUT3}/* > data/${TIME}/${FETCHED_DIR}.txt 
hadoop fs -text ${OUTPUT4}/* > data/${TIME}/${IN_NORM_DIR}.txt 


HOST_STAT="sitemap.stat"
python bin/merge_host.py "data/${TIME}/${PARSED_DIR}.txt" "data/${TIME}/${SELECTED_DIR}.txt" "data/${TIME}/${FETCHED_DIR}.txt" "data/${TIME}/${IN_NORM_DIR}.txt" > data/${TIME}/${HOST_STAT}

CLUSTER_DEST="/user/zhufangze/sitemap/online_monitor_data/web/sitemap_res/${HOST_STAT}.${TIME}"
hadoop fs -rmr ${CLUSTER_DEST}
hadoop fs -put data/${TIME}/${HOST_STAT} ${CLUSTER_DEST}
