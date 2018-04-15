import sys
import numpy as np
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
table1 = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))
table2 = sc.textFile(sys.argv[2], 1).mapPartitions(lambda x: reader(x))
#get the intersectin set of the ith column from table1 and the jth column from table2
i =  sys.argv[3]
j = sys.argv[4]
retult = table1.map(lambda x: x[i]).intersection(table2.map(lambda x: x[j]))



result.saveAsTextFile('inter_query.out')