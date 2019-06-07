# Spark Cheatsheet

## Import a file

### CSV

1. Load as RDD of strings and Remove header

   ```python
   accidentLines = sc.textFile("accidents_2016.csv").filter(lambda line: not line.startswith("accidentIndex"))
   ```

2. Preview RDD

   ```python
   # Get the total number of lines
   accidentLines.count()
   
   # Get the first line using 'first' function
   retailLines.first()
   
   # Get the first two lines using 'take' function
   retailLines.take(2)
   
   # Random sample from dataset
   retailLines.sample(fraction = 0.01, withReplacement=False).collect()
   ```

3. Split each line into fields

   ```python
   accidentFields = accidentLines.map(lambda line: line.split(","))
   ```

4. Remove malformed lines

   ```python
   accidentFields = accidentFields.filter(lambda x: len(x) == 11)
   accidentFields = accidentFields.filter(lambda x: len(x[0]) == 13)
   ```

5. Create dataframe

   **Note: convert specific fields using int(), float(), datetime.strptime(date, dateformat)**

   ```python
   accidentRows = accidentFields.map(lambda x: Row(accidentIndex = x[0],
                                                 accidentSeverity = x[1],
                                                 numVehicles = int(x[2]),
                                                 numCasualties =int(x[3]),
                                                 date = datetime.strptime(x[4], "%d/%m/%Y"),
                                                 dayOfWeek = x[5],
                                                 localAuthority = x[6],
                                                 roadType = x[7],
                                                 speedLimit = x[8],
                                                 lightCondition = x[9],
                                                 weatherCondition = x[10]))
   accidentDF = spark.createDataFrame(accidentRows)
   ```

   **Preview dataframe**

   ```python
   accidentDF.printSchema()
   accidentDF.show()
   Dataframe.describe().toPandas()
   ```

6. Register dataframe as SQL table

   ```python
   accidentDF.createOrReplaceTempView("accidents")
   ```



### CSV (well structured)

```python
retailDF2 = spark.read.csv("2010-12-01.csv",
                            header=True,
                            inferSchema=True,
                            mode="DROPMALFORMED")
```



### JSON

```SQL
flightsJsonDF = spark.read.json("2015-summary.json")
```



## SQL

###Spark SQL interface
```python
# grouppy("<item1>").agg(F.<function>("<item2>").alias("<nickname>"), ... )
retailDF.groupby("country").agg(F.sum("quantity").alias("cntItems"),F.sum("invoice_num").alias("invoice")).show()

#.where("<condition>")
retailDF.groupby("country").agg(F.sum("quantity").alias("cntItems")).where("cntItems > 200 and country != 'Germany'").show()

```



### SQL Query

**Notice: output is dataframe**

```python
spark.sql("""
<SQL query lines>
""").show()
```



### SELECT 

**Notice: if only want to list the different (distinct) values, use DISTINCT**

```SQL
SELECT column1, column2, ...
FROM table_name;

SELECT * FROM table_name

SELECT DISTINCT column1, column2, ...
FROM table_name;
```



### Aggregate Functions

```sql
COUNT() 
MAX()
MIN()
SUM()
AVG()

-- Calculate correlation
CORR(col1, col2)

DATEDIFF(timestamp1, timestamp2)
```



### LIMIT

```SQL
SELECT column_name(s)
FROM table_name
WHERE condition
LIMIT number;
```



### WHERE 

```SQL
SELECT column1, column2, ...
FROM table_name
WHERE condition;

SELECT column1, column2, ...
FROM table_name
WHERE condition1 AND (condition2 OR condition3)...;
```

**Notice: operators in following list can be used in the WHERE Clause**

| Operator     | Description                                      |
| ------------ | ------------------------------------------------ |
| =            | Equal                                            |
| !=, <>       | not equal                                        |
| >, <, >=, <= |                                                  |
| BETWEEN      | Between a certain range                          |
| LIKE         | Search for a pattern                             |
| IN           | To specify multiple possible values for a column |
| AND OR NOT   |                                                  |



### LIKE

```SQL
SELECT column1, column2, ...
FROM table_name
WHERE columnN LIKE pattern;
```

| LIKE Operator                   | Description                                                  |
| ------------------------------- | ------------------------------------------------------------ |
| WHERE CustomerName LIKE 'a%'    | Finds any values that start with "a"                         |
| WHERE CustomerName LIKE '%a'    | Finds any values that end with "a"                           |
| WHERE CustomerName LIKE '%or%'  | Finds any values that have "or" in any position              |
| WHERE CustomerName LIKE '_r%'   | Finds any values that have "r" in the second position        |
| WHERE CustomerName LIKE 'a_%_%' | Finds any values that start with "a" and are at least 3 characters in length |
| WHERE ContactName LIKE 'a%o'    | Finds any values that start with "a" and ends with "o"       |



### IN

```SQL
SELECT column_name(s)
FROM table_name
WHERE column_name IN (value1, value2, ...);

SELECT column_name(s)
FROM table_name
WHERE column_name IN (SELECT STATEMENT);
```



### BETWEEN

```SQL
SELECT column_name(s)
FROM table_name
WHERE column_name BETWEEN value1 AND value2;
```



### GROUP BY & ORDER BY

```SQL
SELECT column1, column2, ...
FROM table_name
ORDER BY column1, column2, ... ASC|DESC;
```

**Notice: Group by need to combine with aggregate functions**

```SQL
SELECT column_name(s)
FROM table_name
WHERE condition
GROUP BY column_name(s)
ORDER BY column_name(s);
```



### HAVING 

**NOTICE: HAVING clause was added to SQL because the WHERE keyword could not be used with aggregate functions.**

```SQL
SELECT column_name(s)
FROM table_name
WHERE condition
GROUP BY column_name(s)
ORDER BY column_name(s);
```



### EXIST & ANY & ALL

- The EXISTS operator is used to test for the existence of any record in a subquery.

- The EXISTS operator returns true if the subquery returns one or more records.

```SQL
SELECT column_name(s)
FROM table_name
WHERE EXISTS
(SELECT column_name FROM table_name WHERE condition);

SELECT column_name(s)
FROM table_name
WHERE column_name operator ANY
(SELECT column_name FROM table_name WHERE condition);

SELECT column_name(s)
FROM table_name
WHERE column_name operator ALL
(SELECT column_name FROM table_name WHERE condition);
```



### JOIN

- **(INNER) JOIN**: Returns records that have matching values in both tables
- **LEFT (OUTER) JOIN**: Return all records from the left table, and the matched records from the right table
- **RIGHT (OUTER) JOIN**: Return all records from the right table, and the matched records from the left table
- **FULL (OUTER) JOIN**: Return all records when there is a match in either left or right table

```SQL
SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate
FROM Orders
INNER JOIN Customers ON Orders.CustomerID=Customers.CustomerID;

SELECT column_name(s)
FROM table1
LEFT JOIN table2
ON table1.column_name = table2.column_name;

SELECT column_name(s)
FROM table1
RIGHT JOIN table2
ON table1.column_name = table2.column_name;

SELECT column_name(s)
FROM table1
FULL OUTER JOIN table2
ON table1.column_name = table2.column_name;

-- COURSE EXAMPLE
SELECT * FROM (
SELECT resource_uri, name, attack, defense, explode(moves) move
FROM pokemon
) t JOIN moves ON t.move.resource_uri = moves.resource_uri
```



### UNION

The UNION operator is used to combine the result-set of two or more SELECT statements.

- Each SELECT statement within UNION must have the same number of columns
- The columns must also have similar data types
- The columns in each SELECT statement must also be in the same order
- UNION selects only distinct values
- UNION ALL to also select duplicate values

```SQL
SELECT column_name(s) FROM table1
UNION
SELECT column_name(s) FROM table2;

SELECT City FROM Customers
UNION ALL
SELECT City FROM Suppliers
ORDER BY City;
```



### NULL VALUE

```SQL
SELECT column_names
FROM table_name
WHERE column_name IS NULL;

SELECT column_names
FROM table_name
WHERE column_name IS NOT NULL;
```



### INSERT INTO

**Specifies both the column names and the values to be inserted**

```SQL
INSERT INTO table_name (column1, column2, column3, ...)
VALUES (value1, value2, value3, ...);
```

**Or for all the columns of the table**

```sql
INSERT INTO table_name
VALUES (value1, value2, value3, ...);
```



### UPDATE

```SQL
UPDATE table_name
SET column1 = value1, column2 = value2, ...
WHERE condition;
```



### DELETE

```SQL
DELETE FROM table_name WHERE condition;
```



### EXPLODE( for JSON)

```sql
SELECT col1, explode(nested_col2) FROM tablename
```



## WINDOW FUNCTION

### RANK

- RANK(col1) over (partition by col2 order by col3 DESC) alias_name
- total rank number == rows number

```SQL
SELECT name, resource_uri, attack, type.name type_name, RANK(attack) over (partition by type.name order by attack DESC) type_attack_rank
FROM (
SELECT name, resource_uri, attack, explode(types) as type
FROM pokemon
)
```

### DENSE RANK

- RANK by sequential number

```SQL
spark.sql("""
SELECT name, resource_uri, attack, type.name type_name, DENSE_RANK(attack) over (partition by type.name order by attack DESC) type_attack_rank
FROM (
SELECT name, resource_uri, attack, explode(types) as type
FROM pokemon
)
""").show()
```

### NILE

```SQL
spark.sql("""
SELECT name, resource_uri, attack, type.name type_name, NTILE(4) over (order by attack DESC) attack_ntile
FROM (
SELECT name, resource_uri, attack, explode(types) as type
FROM pokemon
)
""").show()
```

### MEDIAN

```python
spark.sql("""
SELECT productivity, percentile_approx(domain_vers, 0.5) as domain_vers_median,  percentile_approx(language_vers, 0.5) as language_vers_median
FROM users_vers_pro 
GROUP BY productivity 
ORDER BY productivity
""").show()
```



## Plotting and Display

### HISTOGRAM

```python
spark.sql("""
SELECT attack, count(1) count FROM pokemon
GROUP BY attack
ORDER BY attack
""").toPandas().plot(kind="bar", x="attack", y="count", figsize=(10,5), color="blue")
```

**Notice: Using bins in histogram**

```python
spark.sql("""
SELECT CEIL(attack/10) bin, count(1) count FROM pokemon
GROUP BY CEIL(attack/10)
ORDER BY bin
""").toPandas().plot(kind="bar", x="bin", y="count", figsize=(10,5), color="blue")
```

**Notice: add comparsion in histogram, draw histogram for each group**

```python
spark.sql("""
SELECT (weight > 500) big, CEIL(attack/10) bin, count(1) count FROM pokemon
GROUP BY (weight > 500), CEIL(attack/10)
ORDER BY big, bin
""").toPandas().groupby("big").plot(kind="bar", x="bin", y="count", figsize=(10,5), color="blue")
```

**Notice: multiple classes, Using seaborn Facet Grid**

```python
import seaborn as sns
pd_weight_attack = spark.sql("""
SELECT (CASE
    WHEN weight < 250 THEN "small"
    WHEN weight >= 250 and weight  < 750 THEN "medium"
    ELSE "large"
    END) weight_class, CEIL(attack/10) attack_bin, count(1) count FROM pokemon
GROUP BY weight_class, CEIL(attack/10)
ORDER BY weight_class, attack_bin
""").toPandas()

g = sns.FacetGrid(pd_weight_attack, col="weight_class", col_wrap=3, height=5)
g = g.map(plt.bar, "attack_bin", "count")
```



### BAR 

```python
# Convert to Pandas
pdSales = spark.sql("""
SELECT country, SUM(quantity) cntItems FROM retail
GROUP BY country
""").toPandas()

# Plot
pdSales.plot(kind="bar", x="country", y="cntItems", figsize=(10,5))
```



### PIE

```python
pdSales.set_index("country").plot(kind="pie", y="cntItems", figsize=(10,10))
```



## PARQUET

- Parquet is a columnar file format that is optimized to speed up queries and is a more efficient compared to CSV/JSON.

- In many cases, you may wish to save processed dataframe as Parquet file for fast future use.

```python
# Save the pokemon dataframe as Parquet file
pokemonDF.write.mode("overwrite").parquet("pokemonParquet")

# Load it back using the command
pokemonParq = spark.read.parquet("pokemonParquet")
```



## SPARK UDF(User-Defined Functions)

```python
def camel_case(s):
    return s.title().replace(" ", "")

# Register as a Spark UDF
spark.udf.register("camelcase", camel_case, T.StringType())

# Run in a SQL query on the description fields
spark.sql("""
SELECT name, description, camelcase(description) camel_description 
FROM moves
""").limit(10).toPandas()
```

## Machine Learning

### Main concept

- **ML Dataset**: Spark ML uses the **SchemaRDD** from Spark SQL as a dataset which can hold a variety of data types. E.g., a dataset could have different columns storing text, feature vectors, true labels, and predictions.
- **Transformer**: A Transformer is an algorithm which can transform one SchemaRDD into another SchemaRDD. E.g., an ML model is a Transformer which transforms an RDD with features into an RDD with predictions.
- **Estimator**: An Estimator is an algorithm which can be fit on a SchemaRDD to produce a Transformer. E.g., a learning algorithm is an Estimator which trains on a dataset and produces a model.
- **Pipeline**: A Pipeline chains multiple Transformers and Estimators together to specify an ML workflow.
- **Param**: All Transformers and Estimators now share a common API for specifying parameters.

### Binary Classification

- Categorical features

  - Replace the string with indexes using StringIndexer: transform categorical data into value

    ```python
    si = StringIndexer(inputCol = "job", outputCol =  "job_cat")
    siDF = si.fit(bank_data).transform(bank_data)
    ```

  -  one hot encoding

    ```python
    oh = OneHotEncoderEstimator(inputCols=[si.getOutputCol()], outputCols=["job_classVec"])
    oh.fit(siDF).transform(siDF).limit(20).toPandas()
    ```

  - Build the feature matrix using a pipeline

    - Convert categorical features to one hot encoding
    - Convert the target to zero-one
    - Assembles all the features into a field "features"

    ```python
    categoricalColumns = ['job', 'marital', 'education', 'default', 'housing', 'loan', 'contact', 'poutcome']
    numericCols = ['age', 'balance', 'duration', 'campaign', 'pdays', 'previous']
    
    stages = []
    
    for categoricalCol in categoricalColumns:
        stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index')
        encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
        stages += [stringIndexer, encoder]
    
    label_stringIdx = StringIndexer(inputCol = 'y', outputCol = 'label')
    stages += [label_stringIdx]
    
    assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
    assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
    stages += [assembler]
    ```

    ```python
    pipeline = Pipeline(stages = stages)
    pipelineModel = pipeline.fit(bank_data)
    bank_data_transformed = pipelineModel.transform(bank_data)
    selectedCols = ['label', 'features'] + bank_data.columns
    bank_data_transformed = bank_data_transformed.select(selectedCols)
    bank_data_transformed.printSchema()
    ```

- Evaluation

  - Seperate to train and test set

    ```python
    train, test = bank_data_transformed.randomSplit([0.7, 0.3], seed = 1000)
    train.count(), test.count()
    ```

  - Train the model 

    ```python
    from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
    
    lr = LogisticRegression(featuresCol = 'features', labelCol = 'label', maxIter=10)
    lrModel = lr.fit(train)
    lrModel.coefficients
    ```

  - Prediction 

    ```python
    predictions = lrModel.transform(test)
    predictions.select('age', 'job', 'label', 'rawPrediction', 'prediction', 'probability').limit(20).toPandas()
    ```


