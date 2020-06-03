import findspark
findspark.init()
findspark.find()
import itertools
import pyspark
import sys
import time
import json
import os
from collections import defaultdict
from itertools import combinations
from graphframes import *
from operator import add
from pyspark import SparkContext, SparkConf,SQLContext, StorageLevel


os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")

Conf = pyspark.SparkConf().setAppName('task1').setMaster('local[3]')
sc = SparkContext(conf=Conf)
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)


if __name__ == "__main__":
    if len(sys.argv)!=4:
        print("This function needs 3 input arguments <case number> <support> <input_file_path> <output_file_path>")
        sys.exit(1)
    
    time1=time.time()
    threshold_filter= int(sys.argv[1])
    input_file_path=sys.argv[2]
    output_file_path=sys.argv[3]
    
    def read_csv(x):
        return x.split(',')
    
    def checking_condition(pair,both_dict):
        L=set(both_dict[pair[0]])
        M=set(both_dict[pair[1]])
        length=len(L&M)
        return pair,length


    inputFile = sc.textFile(input_file_path)
    head = inputFile.first()
    inputRDD = inputFile.filter(lambda line: line!= head).map(lambda x:read_csv(x))
    user_dict=inputRDD.sortByKey().map(lambda x:x[0]).distinct().zipWithIndex().collectAsMap()
    reversed_dict= {v:k for k, v in user_dict.items()}
    busi_dict=inputRDD.map(lambda x:(x[1],x[0])).sortByKey().map(lambda x:x[0]).distinct().zipWithIndex().collectAsMap()
    both_rdd=inputRDD.map(lambda x:(user_dict[x[0]],busi_dict[x[1]])).groupByKey().mapValues(list)
    both_dict=dict(both_rdd.collect())
    rdd1=(both_rdd.keys()).cartesian(both_rdd.keys()).filter(lambda x:x[1]!=x[0])
    rdd2=rdd1.map(lambda x:checking_condition(x,both_dict)).filter(lambda x:x[1]>=threshold_filter)
    rdd3=rdd2.map(lambda x:x[0]).sortBy(lambda x:(x[0],x[1]))
    vertices=rdd3.map(lambda x:x[0]).distinct()
    vertices=vertices.map(lambda x:[x])
    vertices_DF=vertices.toDF(["id"])
    edges_DF=rdd3.toDF(["src","dst"])
    graph_frame=GraphFrame(vertices_DF,edges_DF)
    communities_detected=graph_frame.labelPropagation(maxIter=5)
    result=communities_detected.rdd.map(lambda x:(x[1],x[0])).groupByKey().mapValues(list)
    result2=result.map(lambda x:(x[0],[reversed_dict[edge] for edge in x[1]])).map(lambda x:sorted(list(x[1])))
    result3=result2.sortBy(lambda x:(len(x),x))
    
    final_result=result3.map(lambda x: "'" + "', '".join(x) + "'")
    final_output = '\n'.join(final_result.collect())
    with open(output_file_path,'w') as file:
        file.write(final_output)
    time2=time.time()-time1
    print('Duration:',time2)