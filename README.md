# Spark: analytics engine for large-scale data processing

### General
- Spark is lazy: transformations are stored until an action triggers optimization and execution.
- `SparkSession` is the modern entry point, holding both the `SparkContext` and `SQLContext`.
- A *Resilient Distributed Dataset (RDD)* is a low level structure. It is a collection of rows.
- A *Data Frame* is a more high-level structure. Can be thought of as a table. Operations are mostly done on columns.

### Reading and writing
- Reading data is a transformation, and is performed lazily.
- `spark.read` holds the `DataFrameReader` object.
- `inferSchema=True` will make Spark go trough the data twice, which can be costly.
- `DataFrame.write` holds the `DataFrameWriter` object.
- Ordering the data frames before writing in general has no effect, since reading is a distributed operation.

### Distributed data
- A ***shuffle*** is the movement of data between the nodes.
- `.coalesce()` will reduce the number of partitions as a narrow operation, i.e. without shuffling.
- Joining cares about where the data is: to compare, the records have to be on the same machine. Spark will do a shuffle if it's not.

### Columns
- `import pyspark.sql.functions as F`
- `.select()` is like `SELECT` in SQL. It's an entry point to manipulating columns.
- `F.col` is the most versatile way to reference columns of a data frame.
- `.alias()` will rename a column. `.withColumnRenamed()` is an alternative.
- `.orderBy(Columns.desc())` is the same as `.orderBy("col", ascending=False)`.
- `.drop()` will drop columns from a data frame, doing nothing if they don't exist.
- `.cast()` will cast a column into a data type.
- `.withColumn()` will add a column to a data frame, overwriting if exist.
- `.toDF()` will return a data frame with renamed columns using a `*list` of new column names.
- `.select()` can be used to reorder columns; i.e. `df.select(sorted(df.columns))`.

### Values
- `.filter()` and its alias `.where()` are used to filter the vales.
- `~` can be used to negate an entire expression inside a filtering function.
- `F.when(test, true_case).when(another_test, true_case).otherwise(default_case)` will vectorize conditional statements. It returns a column.
- `.dropna(how, thresh, subset)` will drop records with nulls.
- `.fillna(value, subset)` will replace nulls with the given value. Value can be a single value or a dict with column names as keys and column-specific values. Same can be achieved with chaining multiple `.fillna()`.

### Exploratory descriptive statistics
- `.describe(*cols)` calculates summary statistics for numerical and string columns.
- `.summary(*statistics)` is similar but with selectable statistics.
- These are only for exploration! For exact results, use the functions in `sql.functions`.

### Joins
- `left.join(right, on=predicates, how=method)` is a join.
- A string with a column name can replace a predicate if both data frames have a column with the same name.
- Predicates can be combined using boolean operators `|` and `&`; multiple predicates will be joined through a logical and if passed in a list.
- Types of joins are the same as in SQL and are passed as a string: `"inner"`, `"left"`, `"right"`, or `"full"`.
- Left semi-join `"left_semi"` will filter keep the records of the left table that have a match in the right table.
- Left anti-join `"left_anti"` is the opposite, will keep only records that don’t have a match.
- A cross join `"cross"` is the nuclear option that returns the cross product: every possible pair, ignoring the predicate.

### Groups and aggregations
- `.groupby()` returns a `GroupedData` object.
- `.agg()` is used to apply one or more aggregate functions from `pyspark.sql.functions` to `GroupedData`.
- Since `.agg()` uses `F.col()`, it can operate on arbitrary columns which makes it very flexible.
- Another way to return to a data frame is to use a summarizing method such as `.sum()` or `.min()` directly.

### Complex types
- *Complex*, *container*, or *compound* type in Spark is a type that contains other types.
- The complex types in PySpark are *array*, *map*, and *struct*.
- Arrays are containers for values of the same type. Spark will silently up-cast data to the lowest possible type that can hold all values.
- `df.select(F.col("array_col").getItem(index))` will access items from an array column.
- `F.size()` will return the size of a complex type.
- Map is like a typed dictionary: it contains pairs of keys and values, where the keys are all of the same type and the values are all of the same type.
- `F.col("map_column.key")` or `F.col("map_column")["key"]` will access items from a map column.
- Struct is like a JSON: keys are strings, values can be any type; the number of fields and their names are fixed for every row.
- Struct is like a nested data frame.
- `F.explode()` will create a separate row for each element of an array or a map. `F.posexplode()` will do the same, additionally adding a column with the position of the element.
- `F.explode_outer()` is the same, exept for empty arrays/maps it returns the row with a `null` value.
- `F.collect_list()` and `F.collect_set()` do the opposite of `F.explode()`: they gather values into arrays.
- To collect a mp, pass two collected arrays to `F.map_from_arrays()`.
- `F.struct()` creates a struct column out of multiple columns.

### Configuration
- An existing session does not allow for changing JVM configuration; a full reset is required.
- `spark.sparkContext.setLogLevel()` can make Spark less chatty.

### Workflow
- Plan the steps on paper.
- Ingest, print the structure with `.printSchema()`, take a look at a sample with `.show(n, truncate, vertical)`.
- `.dtypes` contains a list of column names and their types.
