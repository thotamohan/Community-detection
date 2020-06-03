import findspark
findspark.init()
findspark.find()
import itertools
import pyspark
import sys
import time
import operator 
import json
from collections import defaultdict
from itertools import combinations
from pyspark import SparkConf, SparkContext, StorageLevel, SQLContext
from operator import add

conf = SparkConf()
conf.setMaster('local')
conf.set("spark.executor.memory", "4g")
conf.set("spark.driver.memory", "4g")
sc = SparkContext.getOrCreate(conf)

def read_csv(x):
        return x.split(',')
    
def checking_condition(pair,both_dict):
    L=set(both_dict[pair[0]])
    M=set(both_dict[pair[1]])
    length=len(L&M)
    return pair,length


def bfs_inner(present,Dictionary,visited_list,queue):
    for adj in Dictionary[present] :
        if adj not in visited_list:
            a=visited_list[present][0]+1
            b=present
            visited_list[adj]=(a, [b])
            queue.append(adj)
        elif 1+visited_list[present][0]==visited_list[adj][0]: 
            m=visited_list[adj][1]
            m.append(present)
    return visited_list


def Breadth_FS(root,adjacentDict):
    queue = []
    visited_list={}
    visited_list[root] = (0, [])
    queue.append(root)
    if not adjacentDict[root]: 
        return visited_list
    while len(queue)!=0:
        present = queue.pop(0)
        visited_list=bfs_inner(present,adjacentDict,visited_list,queue)
    return visited_list


def function_1(vertex,Dictionary_object, level,node_of_parent):
    bfs_output = Breadth_FS(vertex,Dictionary_object)
    sort_bread_fs=sorted(bfs_output.items(), key = lambda pair: -pair[1][0])
    for items in sort_bread_fs:
        node_of_parent.extend(items[1][1])
        level.setdefault(items[1][0], []).append((items[0], items[1][1]))
    return level,node_of_parent


def function_2(level,length_level,path_obtained):
    for l in range(0, length_level):
        for (child, parents) in level[l]: 
            if parents: 
                list1=[]
                for items in parents:
                    list1.append(path_obtained[items])
                path_obtained[child] = sum(list1)
            else:
                path_obtained[child] = 1  
    return path_obtained


def setting_func3(parent,child):
    if parent>child:
        m=child
        n=parent
    else:
        m=parent
        n=child
    return m,n   

def func_3_inner(dictionary,value,key):
    try:
        dictionary[key] = dictionary[key]+value
    except: 
        dictionary[key] = value
    return dictionary


def function_3A(parentnode,level, weight_nodes,shortest_path,between_value):
    for things in level.items():
        for (child, parents) in things[1]: 
            if child not in parentnode: 
                weight_nodes[child] = 1
            else: 
                weight_nodes[child] += 1
            
            list1=[]
            for items in parents:
                list1.append(shortest_path[items])
            allparents=sum(list1)
        
            for parent in parents:
                value1=weight_nodes[child]*float(shortest_path[parent])/allparents
                weight_nodes=func_3_inner(weight_nodes,value1,parent)
                

                value_1,value_2=setting_func3(parent,child)
                
                value2=weight_nodes[child]*float(shortest_path[parent])/allparents
                between_value=func_3_inner(between_value,value2,(value_1, value_2))
                
    return between_value


def Betweeness3(Dictionary):
    betw_dict = {}
    vertices=set(list(Dictionary.keys()))
    for vertex in vertices:
        shortest_path = {}
        level = {}
        parentnode = []
        level,parentnode=function_1(vertex,Dictionary, level,parentnode)
        
        length_level=len(level)
        shortest_path=function_2(level,length_level,shortest_path)
        
        parentnode = set(parentnode)
        nodeweight = {}
        betw_dict=function_3A(parentnode,level, nodeweight,shortest_path,betw_dict)
    
    for things in betw_dict.items():
        betw_dict[things[0]]=things[1]/2
    return {key:value for (key,value) in betw_dict.items()}


def checking_dictionary(item):
    if not dictionary_check[item]: 
        del dictionary_check[item]
    return dictionary_check


def modularity_calc(com,dictionary_check,modu,calculated_value):
    value1=0
    k=0
    while com:
        i = com.pop()
        value1=value1+len(dictionary_check[i] & com)
        k=k+sum([len(dictionary_check[j]) for j in com])*len(dictionary_check[i])
    calc_1=(k/calculated_value)
    modu=modu+value1-(calc_1)
    return modu

def Edge_deletion(dictionary_check, value1, value2): 
    dictionary_check[value1].remove(value2)
    dictionary_check=checking_dictionary(value1)
    dictionary_check[value2].remove(value1)
    dictionary_check=checking_dictionary(value2)
    return dictionary_check



def Connectivity(dictionary_check):
    community = []
    vertex = set(list(dictionary_check.keys()))
    while vertex:
        Nodes_connection = set(Breadth_FS(list(vertex)[0],dictionary_check).keys())
        community.append(Nodes_connection)
        vertex=vertex-Nodes_connection
    return community



if __name__ == "__main__":
    if len(sys.argv)!=5:
        print("This function needs 4 input arguments <case number> <support> <input_file_path> <output_file_path>")
        sys.exit(1)
    
    threshold_filter= int(sys.argv[1])
    input_file_path=sys.argv[2]
    output_file_path=sys.argv[3]
    community_output_file=sys.argv[4]
    
    
    time1=time.time()
    inputFile = sc.textFile(input_file_path)
    head = inputFile.first()
    inputRDD = inputFile.filter(lambda line: line!= head).map(lambda x:read_csv(x))
    user_dict=inputRDD.sortByKey().map(lambda x:x[0]).distinct().zipWithIndex().collectAsMap()
    reversed_dict={v:k for k,v in user_dict.items()}
    busi_dict=inputRDD.map(lambda x:(x[1],x[0])).sortByKey().map(lambda x:x[0]).distinct().zipWithIndex().collectAsMap()
    both_rdd=inputRDD.map(lambda x:(user_dict[x[0]],busi_dict[x[1]])).groupByKey().mapValues(list)
    both_dict=dict(both_rdd.collect())
    rdd1=(both_rdd.keys()).cartesian(both_rdd.keys()).filter(lambda x:x[1]!=x[0])
    rdd2=rdd1.map(lambda x:checking_condition(x,both_dict)).filter(lambda x:x[1]>=threshold_filter)
    rdd3=rdd2.map(lambda x:x[0]).sortBy(lambda x:(x[0],x[1]))
    dictionary_check=dict(rdd3.groupByKey().mapValues(set).collect())

    between=Betweeness3(dictionary_check)
    between=dict(between)

    between_temp={}
    for keys,values in between.items():
        k1=reversed_dict[keys[0]]
        k2=reversed_dict[keys[1]]
        between_temp[(k1,k2)]=values

    detected_betweeness=[]
    temp_a=[]
    for items in between_temp.items():
        temp_a.append(list(items))
        detected_betweeness.append(temp_a)
    detected_betweeness=sorted(detected_betweeness[0],key=lambda x:(-x[1],x[0]))

    list_b=[]
    for items in detected_betweeness:
        list_b.append(str(items[0]) + ', ' + str(items[1]))

    with open(output_file_path, 'w') as file:
        file.write('\n'.join(list_b))
        file.close()
        
        
    max_modu = -float('inf')
    vertexs = list(dictionary_check.keys())
    Final_Community = []
    while dictionary_check:
        community = []
        vertex = set(list(dictionary_check.keys()))
        while vertex:
            connectNodes = set(Breadth_FS(list(vertex)[0],dictionary_check).keys())
            community.append(connectNodes)
            vertex -= connectNodes
        modu = 0
        calculated_value = sum([ edges for (v, edges) in { v: len(edges) for (v, edges) in dictionary_check.items() }.items()])
        for com in community:
            if len(com) == 1: 
                continue
            modu=modularity_calc(com,dictionary_check,modu,calculated_value)

        if modu > max_modu:
            max_modu = modu
            Final_Community= Connectivity(dictionary_check)

        betweenes_calc = Betweeness3(dictionary_check)
        sorted_d = sorted(betweenes_calc.items(), key=operator.itemgetter(1),reverse=True)
        Edge_deletion(dictionary_check,sorted_d[0][0][0], sorted_d[0][0][1]) 


    final_list = []
    for items in Final_Community:
        temp = []
        for values in items:
            values=reversed_dict[values]
            temp.append(values)
        final_list.append(temp)

    detected_communities=[]
    temp2=[]
    for items in final_list:
        temp2.append(sorted(list(items)))
        detected_communities.append(temp2)
    detected_communities=sorted(detected_communities[0],key=lambda x:(len(x),x))

    list_a=[]
    for communities in detected_communities:
        list_a.append(str(communities)[1:-1])

    with open(community_output_file, 'w') as file:
        file.write('\n'.join(list_a))
        file.close()
    
    time_took=time.time()-time1
    print('Duration:',time_took)