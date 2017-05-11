import sys
from pyspark import SparkContext

input  = sys.argv[1]
output = sys.argv[2]
sitemap_type = sys.argv[3]

sc = SparkContext()
rdd_0 = sc.textFile(input, use_unicode=False)
rdd_1 = rdd_0 \
  .filter(lambda r: len(r.split("\t"))==8) \
  .filter(lambda r: r.split("\t")[-1].strip()==sitemap_type) \
  .map(lambda r: (r.split("\t")[1], 1) ) \
  .reduceByKey(lambda acc, n: acc+n)
rdd_1.repartition(10).saveAsTextFile(output)
sc.stop()
