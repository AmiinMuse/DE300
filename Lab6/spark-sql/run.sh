docker run -v $(pwd):/tmp/spark-sql -it \
           -p 8888:8888 \
           --name spark-sql-container \
           jupyter/pyspark-notebook:latest
