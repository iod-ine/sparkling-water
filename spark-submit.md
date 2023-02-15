## `spark-submit`

A script to launch application on the cluster.

#### Setting Spark configuration
```shell
spark-submit \
  --conf "spark.pyspark.python=/path/to/python" \
  --conf "spark.pyspark.driver.python=/path/to/python" \
  app.py
```
