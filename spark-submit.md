## `spark-submit`

A script to launch application on the cluster.

### Option explanaitons
- `--deploy-mode`: where to laucn the driver process.
The driver can run on the machine that makes the call to `spark-submit` or it can run on one of the cluster nodes.

### Examples
#### Setting Spark configuration
```shell
spark-submit \
  --conf "spark.pyspark.python=/path/to/python" \
  --conf "spark.pyspark.driver.python=/path/to/python" \
  app.py
```
