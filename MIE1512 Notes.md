# Spark

## Load from file

- Load from json

  ```py
  pokemonDF_raw = spark.read.json("pokemon.json")
  ```

## Display Schema

- Inspect the inferred schema

  ```py
  pokemonDF_raw.printSchema()
  ```

## Using SQL queries

- Tempview

  ```py
  pokemonDF_raw.createOrReplaceTempView("pokemon_raw")
  ```

- SQL

  ```SQl
  //CAST(created as timestamp)
  
  pokemonDF = spark.sql("""
  SELECT abilities, attack, catch_rate, CAST(created as timestamp), defense, descriptions, egg_cycles, egg_groups, ev_yield, evolutions, exp, growth_rate, happiness, CAST(height as bigint), hp, male_female_ratio, CAST(modified as timestamp), moves, name, national_id, pkdx_id, resource_uri, sp_atk, sp_def, species, speed, sprites, total, types, CAST(weight as bigint)
  FROM pokemon_raw
  """)
  
  // Select rows with specified limit
  spark.sql("""
  SELECT * FROM pokemon
  LIMIT 10
  """).show()
  
  //COUNT(1) count item number
  //AVG() calculate average
  //MAX()
  //MIN()
  //SUM()
  spark.sql("""
  SELECT COUNT(1) num_pokemons, AVG(attack) avg_attack, MAX(defense) max_defense, MIN(speed) min_speed FROM pokemon
  """).show()
  
  //CORR() Calculate correlation
  spark.sql("""
  SELECT CORR(attack, weight) corr_attack_weight
  FROM pokemon
  """).show()
  
  //GROUP BY define the group policy
  //ORDER BY define the order
  spark.sql("""
  SELECT attack, count(1) count FROM pokemon
  GROUP BY attack
  ORDER BY attack
  """).show()
  
  //Add filter with boolean statement
  spark.sql("""
  SELECT (weight > 500) big, CEIL(attack/10) bin, count(1) count FROM pokemon
  GROUP BY (weight > 500), CEIL(attack/10)
  ORDER BY big, bin
  """).toPandas().groupby("big").plot(kind="bar", x="bin", y="count", figsize=(10,5), color="blue")
  
  //Add filter with multiple cases
  spark.sql("""
  SELECT (CASE
      WHEN weight < 250 THEN "small"
      WHEN weight >= 250 and weight  < 750 THEN "medium"
      ELSE "large"
      END) weight_class, CEIL(attack/10) attack_bin, count(1) count FROM pokemon
  GROUP BY weight_class, CEIL(attack/10)
  ORDER BY weight_class, attack_bin
  """).show()
  ```

- Nested data in Json

  ```sql
  //Select the first item of abilities
  spark.sql("""
  SELECT abilities[0] FROM pokemon
  LIMIT 10
  """).toPandas()
  
  //Show the schema of abilities
  pokemonDF.schema["abilities"]
  
  //firstitem.name 
  spark.sql("""
  SELECT abilities[0].name name, abilities[0].resource_uri resource_uri FROM pokemon
  LIMIT 10
  """).toPandas()
  
  //Explode function to spread out the nested data
  //explode(abilities) for each item from abilities, create a new row
  spark.sql("""
  SELECT name, explode(abilities) FROM pokemon
  LIMIT 10
  """).toPandas()
  
  //嵌套一个
  spark.sql("""
  SELECT pokemon_type.name, CEIL(height/10) height_bin, count(1) FROM
  (
  SELECT height, explode(types) pokemon_type
  FROM pokemon
  )
  GROUP BY pokemon_type.name, height_bin
  ORDER BY pokemon_type.name, height_bin
  """).show()
  ```

- Join tables

  ```sql
  spark.sql("""
  SELECT * FROM (
  SELECT resource_uri, name, attack, defense, explode(moves) move
  FROM pokemon
  ) t JOIN moves ON t.move.resource_uri = moves.resource_uri
  """).show()
  ```

- Windows function

  ```sql
  \\DENSE_RANK() over(partition by $a$ order by $b$)
  spark.sql("""
  SELECT name, count(1) cntTopRanked
  FROM (
      SELECT * 
      FROM (
          SELECT name, resource_uri, attack, type.name type_name, DENSE_RANK(attack) over (partition by type.name order by attack DESC) type_attack_rank
          FROM (
              SELECT name, resource_uri, attack, explode(types) as type
              FROM pokemon
          )
      )
      WHERE type_attack_rank <= 3
  )
  GROUP BY name
  ORDER by cntTopRanked DESC
  """).show()
  
  \\NTILE()
  spark.sql("""
  SELECT name, resource_uri, attack, type.name type_name, NTILE(4) over (order by attack DESC) attack_ntile
  FROM (
  SELECT name, resource_uri, attack, explode(types) as type
  FROM pokemon
  )
  """).show()
  ```


## To Pandas

- Display Histogram

  ```sql
  spark.sql("""
  SELECT attack, count(1) count FROM pokemon
  GROUP BY attack
  ORDER BY attack
  """).toPandas().plot(kind="bar", x="attack", y="count", figsize=(10,5), color="blue")
  
  //with bins
  spark.sql("""
  SELECT CEIL(attack/10) bin, count(1) count FROM pokemon
  GROUP BY CEIL(attack/10)
  ORDER BY bin
  """).show()
  ```


- Seaborn

  ```py
  g = sns.FacetGrid(pd_weight_attack, col="weight_class", col_wrap=3, height=5)
  g = g.map(plt.bar, "attack_bin", "count")
  ```


## Window function

```sql
//RANK
spark.sql("""
SELECT name, resource_uri, attack, type.name type_name, RANK(attack) over (partition by type.name order by attack DESC) type_attack_rank
FROM (
SELECT name, resource_uri, attack, explode(types) as type
FROM pokemon
)
""").show()

//DENSE RANK
spark.sql("""
SELECT name, resource_uri, attack, type.name type_name, DENSE_RANK(attack) over (partition by type.name order by attack DESC) type_attack_rank
FROM (
SELECT name, resource_uri, attack, explode(types) as type
FROM pokemon
)
""").show()

//WHERE FILTER, need SELECT * WHERE ...
spark.sql("""
SELECT * 
FROM (
    SELECT name, resource_uri, attack, type.name type_name, DENSE_RANK(attack) over (partition by type.name order by attack DESC) type_attack_rank
    FROM (
        SELECT name, resource_uri, attack, explode(types) as type
        FROM pokemon
    )
)
WHERE type_attack_rank <= 3
""").show()

//NTILE(4)
spark.sql("""
SELECT name, resource_uri, attack, type.name type_name, NTILE(4) over (order by attack DESC) attack_ntile
FROM (
    SELECT name, resource_uri, attack, explode(types) as type
    FROM pokemon
)
""").show()
```

## SPARK UDF

```python
def camel_case(s):
    return s.title().replace(" ", "")
    
spark.udf.register("camelcase", camel_case, T.StringType())

spark.sql("""
SELECT name, description, camelcase(description) camel_description 
FROM moves
""").limit(10).toPandas()
 
```

