from pyspark import SparkContext
from pyspark.sql import SQLContext
import sys


if __name__ == "__main__":

        # logFile = "/export"

        sc = SparkContext()
        sqlContext = SQLContext(sc)

        logData = sqlContext.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
        # logData.show()

	# for the 1st dataset
        # result = logData.orderBy("cca2", "timestamp")
        
	# for the 2nd dataset
	result = logData.orderBy("Hospital County", "Facility Name")
	
	result.show()
        result.write.format("csv").option("header","true").save(sys.argv[2])
