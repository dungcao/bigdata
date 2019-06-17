from __future__ import print_function

import os, math
from pyspark.sql import SparkSession


# Map 1: extract words from a file
# input: file_name
# output: (term, doc_name), 1

def map1(doc_name):
    file = open(doc_name)
    while True:
        line = file.readline()
        if line == '':
            break
        terms = line.split()
        for term in terms:
            yield (term, doc_name), 1


# Reduce 1: calculate tf for terms in each document 
# input: (term, doc_name), 1
# output: (term, doc_name), n

def reduce1(x, y):
    return x + y


# Map 2
# input: (term, doc_name), n
# output: doc_name, ([(term, n)], n)

def map2(x):
    term = x[0][0]
    doc_name = x[0][1]
    n = x[1]
    return doc_name, ([(term, n)], n)


# Reduce 2
# input: doc_name, ([(term, n)], n)
# output: doc_name, ([list of [term, n]], N)

def reduce2(x, y):
    return (x[0] + y[0], x[1] + y [1])

# Map 3
# input: doc_name, ([list of [term, n]], N)
# output: term, ([(n, doc_name, N)], n)

def map3(x):
    doc_name = x[0]
    terms = x[1][0]
    N = x[1][1]
    for item in terms:
        term = item[0]
        n = item[1]
        yield term, ([(n, doc_name, N)], 1)

# Reduce 3:
# input: term, ([(n, doc_name, N)], 1)
# output: term, ([(n, doc_name, N)], m)

def reduce3(x, y):
    return (x[0] + y[0], x[1] + y[1])


# Final map
# input: term, ([(n, doc_name, N)], m)
# output: term, (doc_name, n, N, m, tf score, idf score, tf-idf score)

def map4(x, D):
    term = x[0]
    docs = x[1][0]
    m = x[1][1]

    for item in docs:
        n = item[0]
        doc_name = item[1]
        N = item[2]

        tf = float(n) / N
        idf = math.log(float(D) / m)
        tfidf = tf * idf

        yield term, (doc_name, n, N, m, tf, idf, tfidf)

# main program
if __name__ == "__main__":

    # create spark context
    spark = SparkSession.builder.appName("TfIdf-Exercise").getOrCreate()

    # retrieve all files in the data input directory
    input_folder = "input_txt"
    input_files = [os.path.join(input_folder, file_name) for file_name in os.listdir(input_folder)]

    # get D = total number of documents
    D = len(input_files)

    
    # to avoid cases where a term appearing in all documents
    # idf = log(D/m) = log(1) = 0
    # we increase D by 1
    
    D = D + 1

    # put list of files into rdd for parallel processing
    files = spark.sparkContext.parallelize(input_files)

    # calculate terms and their number of occurrences in each document (tf)
    terms_in_docs = files.flatMap(lambda x: map1(x)).reduceByKey(lambda x, y: reduce1(x, y))

    # calculate number of occurrences of all terms in each document (N)
    docs_have_terms = terms_in_docs.map(lambda x: map2(x)).reduceByKey(lambda x, y: reduce2(x, y))

    # calculate number of documents containing specific terms (idf)
    terms_all = docs_have_terms.flatMap(lambda x: map3(x)).reduceByKey(lambda x, y: reduce3(x, y))

    # calculate tf-idf
    terms_final = terms_all.flatMap(lambda x: map4(x, D))

    # call collect() to output the result
    print(terms_final.collect())

    # stop spark context
    spark.stop()