docker run -v /home/ubuntu/DE300/Homework3:/tmp/Homework3 -it \
           -p 8888:8888 \
           --name spark-sql-container \
	   pyspark-image