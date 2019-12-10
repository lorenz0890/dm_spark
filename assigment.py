from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from numpy import genfromtxt
from pyspark.mllib.linalg import *
from pyspark.mllib.linalg.distributed import *

if __name__ == "__main__":

    #Init
    #https://www.programcreek.com/python/example/86705/pyspark.SparkContext
    sc_conf = SparkConf()
    sc_conf.setAppName("dm")
    sc_conf.setMaster('local')
    sc_conf.set('spark.executor.memory', '2g')
    sc_conf.set('spark.executor.cores', '4')
    sc_conf.set('spark.cores.max', '4')
    sc_conf.set('spark.logConf', True)
    print(sc_conf.getAll())

    sc = None
    try:
        sc.stop()
        sc = SparkContext(conf=sc_conf)
    except:
        sc = SparkContext(conf=sc_conf)

    #Load data
    #https://stackoverflow.com/questions/3518778/how-do-i-read-csv-data-into-a-record-array-in-numpy

    spark = SparkSession(sc)

    A = spark.read.load('./data/matrix_A.csv', format='csv')
    B = spark.read.load('./data/matrix_B.csv', format='csv')

    A.registerTempTable("A_tab")
    B.registerTempTable("B_tab")

    A.show()
    B.show()

    #https://therightjoin.wordpress.com/2014/07/18/matrix-multiplication-using-sql/
    #https://notes.rohitagarwal.org/2013/06/07/sparse-matrix-multiplication-using-sql.html
    J = spark.sql("SELECT A_tab._c0, B_tab._c1, SUM(A_tab._c2 * B_tab._c2) "+
                  "FROM A_tab, B_tab "+
                  "WHERE A_tab._c0 = B_tab._c0 "+ #should be A_tab._c1
                  "GROUP BY A_tab._c0, B_tab._c1")

    J.show()

    C = spark.sql("SELECT A._c0, "+
                  "B._c1, "+
                  "SUM(A._c2 * B._c2) AS _c2 "+
                  "FROM A_tab A "+
                  "INNER JOIN B_tab B "+
                  "ON A._c1 = B._c1 "+
                  "GROUP BY A._c0, B._c1")

    C.show()

