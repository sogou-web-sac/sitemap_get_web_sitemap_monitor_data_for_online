import sys
from pyspark import SparkContext

def read_rdd(input_path):
  rdd = sc.textFile(input_path, use_unicode=False)
  rdd = rdd \
    .filter(lambda r: len(r.split())==3) \
    .map(lambda r: (r.split("\t")[1].strip(), 1) )
  return rdd

input_sitemap_spider_output = sys.argv[1]
input = sys.argv[2]
output = sys.argv[3]
sitemap_type = sys.argv[4]

sc = SparkContext()
rdd_0 = sc.textFile(input_sitemap_spider_output, use_unicode=False)
rdd_0 = rdd_0 \
  .filter(lambda r: len(r.split("\t"))==8) \
  .filter(lambda r: r.split("\t")[-1].strip()==sitemap_type) \
  .map(lambda r: (r.split("\t")[3], r.split("\t")[1])) \
  .distinct()

rdd = read_rdd(input)

rdd = rdd_0.join(rdd).map(lambda p: p[1]).reduceByKey(lambda acc,x: acc+x)

rdd.repartition(10).saveAsTextFile(output)
sc.stop()
