// Databricks notebook source
// STARTER CODE - DO NOT EDIT THIS CELL
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
val customSchema = StructType(Array(StructField("lpep_pickup_datetime", StringType, true), StructField("lpep_dropoff_datetime", StringType, true), StructField("PULocationID", IntegerType, true), StructField("DOLocationID", IntegerType, true), StructField("passenger_count", IntegerType, true), StructField("trip_distance", FloatType, true), StructField("fare_amount", FloatType, true), StructField("payment_type", IntegerType, true)))

// COMMAND ----------

// STARTER CODE - YOU CAN LOAD ANY FILE WITH A SIMILAR SYNTAX.
val df = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema)
   .load("/FileStore/tables/nyc_tripdata.csv") // the csv file which you want to work with
   .withColumn("pickup_datetime", from_unixtime(unix_timestamp(col("lpep_pickup_datetime"), "MM/dd/yyyy HH:mm")))
   .withColumn("dropoff_datetime", from_unixtime(unix_timestamp(col("lpep_dropoff_datetime"), "MM/dd/yyyy HH:mm")))
   .drop($"lpep_pickup_datetime")
   .drop($"lpep_dropoff_datetime")

// COMMAND ----------

// LOAD THE "taxi_zone_lookup.csv" FILE SIMILARLY AS ABOVE. CAST ANY COLUMN TO APPROPRIATE DATA TYPE IF NECESSARY.

// ENTER THE CODE BELOW
val df_taxi_zone = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .option("inferSchema" , "true")
   .load("/FileStore/tables/taxi_zone_lookup.csv")

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Some commands that you can use to see your dataframes and results of the operations. You can comment the df.show(5) and uncomment display(df) to see the data differently. You will find these two functions useful in reporting your results.
// display(df)
df.show(5) // view the first 5 rows of the dataframe

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Filter the data to only keep the rows where "PULocationID" and the "DOLocationID" are different and the "trip_distance" is strictly greater than 2.0 (>2.0).

// VERY VERY IMPORTANT: ALL THE SUBSEQUENT OPERATIONS MUST BE PERFORMED ON THIS FILTERED DATA

val df_filter = df.filter($"PULocationID" =!= $"DOLocationID" && $"trip_distance" > 2.0)
df_filter.show(5)

// COMMAND ----------

// PART 1a: The top-5 most popular drop locations - "DOLocationID", sorted in descending order - if there is a tie, then one with lower "DOLocationID" gets listed first
// Output Schema: DOLocationID int, number_of_dropoffs int 

// Hint: Checkout the groupBy(), orderBy() and count() functions.

// ENTER THE CODE BELOW
//df_filter.groupBy("DOLocationID").count().withColumnRenamed("count","distinct_name").sort(desc("count")).show()

//df.orderBy($"A", $"B".desc).show

/*
val df3 = df2.selectExpr("cast(age as int) age",
    "cast(isGraduated as string) isGraduated",
    "cast(jobStartDate as string) jobStartDate")
  df3.printSchema()
  df3.show(false)
  
  //Using selectExpr()
  df.selectExpr("cast(salary as int) salary","isGraduated")
  df.selectExpr("INT(salary)","isGraduated")
  */

val df_1a = df_filter.groupBy("DOLocationID").count().withColumnRenamed("count","number_of_dropoffs").orderBy($"number_of_dropoffs".desc,$"DOLocationID").selectExpr("DOLocationID","cast(number_of_dropoffs as int) number_of_dropoffs")

//val df_1a_test = df_1a.selectExpr("DOLocationID","cast(number_of_dropoffs as int) number_of_dropoffs")


//df_filter.groupBy("DOLocationID").count().withColumnRenamed("count","number_of_dropoffs").orderBy(desc("count")).show(5)

//df_filter.createOrReplaceTempView("df_filter")
//val df_1a = spark.sql("select DOLocationID,int(count(*)) as number_of_dropoffs from df_filter group by DOLocationID order by number_of_dropoffs desc,DOLocationID //asc  limit 5")

df_1a.printSchema
df_1a.limit(5).show()

// COMMAND ----------

// PART 1b: The top-5 most popular pickup locations - "PULocationID", sorted in descending order - if there is a tie, then one with lower "PULocationID" gets listed first 
// Output Schema: PULocationID int, number_of_pickups int

// Hint: Code is very similar to part 1a above.

// ENTER THE CODE BELOW
//df_filter.createOrReplaceTempView("df_filter")

val df_1b = df_filter.groupBy("PULocationID").count().withColumnRenamed("count","number_of_pickups").orderBy($"number_of_pickups".desc,$"PULocationID").selectExpr("PULocationID","cast(number_of_pickups as int) number_of_pickups")

//val df_1b = spark.sql("select PULocationID,int(count(*)) as number_of_pickups from df_filter group by PULocationID order by number_of_pickups desc,PULocationID //asc  limit 5")

//df_1b.printSchema
display(df_1b.limit(5))


// COMMAND ----------

// PART 2: List the top-3 locations with the maximum overall activity, i.e. sum of all pickups and all dropoffs at that LocationID. In case of a tie, the lower LocationID gets listed first.
// Output Schema: LocationID int, number_activities int

// Hint: In order to get the result, you may need to perform a join operation between the two dataframes that you created in earlier parts (to come up with the sum of the number of pickups and dropoffs on each location). 

// ENTER THE CODE BELOW

//val df_part2 = spark.sql("select c.PULocationID as LocationID, int(c.number_of_pickups+c.number_of_dropoffs) as number_activities from (select a.PULocationID, a.number_of_pickups,b.DOLocationID,b.number_of_dropoffs from (select PULocationID,count(*) as number_of_pickups from df_filter group by PULocationID) a join (select DOLocationID,count(*) as number_of_dropoffs from df_filter group by DOLocationID) b on a.PULocationID = b.DOLocationID) c order by number_activities desc limit 3")

/*
empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"inner")
    .show(false)
*/

val df_part2 = df_1a.join(df_1b,df_1a("DOLocationID") === df_1b("PULocationID")).selectExpr("PULocationID as LocationID","number_of_pickups+number_of_dropoffs as number_activities").orderBy($"number_activities".desc)

display(df_part2.limit(3))

//.show()

//df_part2.printSchema

// COMMAND ----------


// PART 3: List all the boroughs in the order of having the highest to lowest number of activities (i.e. sum of all pickups and all dropoffs at that LocationID), along with the total number of activity counts for each borough in NYC during that entire period of time.
// Output Schema: Borough string, total_number_activities int

// Hint: You can use the dataframe obtained from the previous part, and will need to do the join with the 'taxi_zone_lookup' dataframe. Also, checkout the "agg" function applied to a grouped dataframe.

// ENTER THE CODE BELOW

//df_taxi_zone.createOrReplaceTempView("df_taxi_zone")

//val df_part3 = spark.sql("select e.Borough, int(sum(d.number_activities)) as total_number_activities from (select c.PULocationID as LocationID, int(c.number_of_pickups+c.number_of_dropoffs) as number_activities from (select a.PULocationID, a.number_of_pickups,b.DOLocationID,b.number_of_dropoffs from (select PULocationID,count(*) as number_of_pickups from df_filter group by PULocationID) a join (select DOLocationID,count(*) as number_of_dropoffs from df_filter group by DOLocationID) b on a.PULocationID = b.DOLocationID) c) d join df_taxi_zone e on d.LocationID = e.LocationID group by e.Borough order by total_number_activities desc")

/*
val df_part2 = df_1a.join(df_1b,df_1a("DOLocationID") === df_1b("PULocationID")).selectExpr("PULocationID as LocationID","number_of_pickups+number_of_dropoffs as number_activities").orderBy($"number_activities".desc)
*/

val df_part3 = df_part2.join(df_taxi_zone,df_part2("LocationID") === df_taxi_zone("LocationID")).groupBy("Borough").agg(sum("number_activities").as("total_number_activities")).orderBy($"total_number_activities".desc).selectExpr("Borough","cast(total_number_activities as int) total_number_activities")

display(df_part3)
//.show(10)

// COMMAND ----------

// PART 4: List the top 2 days of week with the largest number of (daily) average pickups, along with the values of average number of pickups on each of the two days. The day of week should be a string with its full name, for example, "Monday" - not a number 1 or "Mon" instead.
// Output Schema: day_of_week string, avg_count float

// Hint: You may need to group by the "date" (without time stamp - time in the day) first. Checkout "to_date" function.
/*
List the top 2 days of week with the largest number of (daily) average pickups, along with the
values of the average number of pickups on each of the two days in descending order. Here, the
average pickup is calculated by taking an average of the number of pickups on different dates falling
on the same day of the week. For example, 02/01/2021, 02/08/2021 and 02/15/2021 are all Mondays,
so the average pickups for these is the sum of the pickups on each date divided by 3. An example
output is shown below.

date_format(col("Input"), "MM-dd-yyyy").as("format")
to_date(col("Input"), "MM/dd/yyyy").as("to_date")

date_format(tpep_pickup_datetime, 'EEE') as day_of_week
.withColumn("week_day_full", date_format(col("input_timestamp"), "EEEE"))
*/
// ENTER THE CODE BELOW

val df_part4 = df_filter.select(col("pickup_datetime"),date_format(col("pickup_datetime"),"MM/dd/yyyy").as("day_of_week"),date_format(to_date(to_timestamp(col("pickup_datetime")),"MM-dd-yyyy"), "EEEEE").alias("pickup_datetime_1"),col("PULocationID"),to_date(to_timestamp(col("pickup_datetime")),"MM-dd-yyyy").as("to_date_val"))

val df_part4a = df_part4.groupBy("to_date_val").agg(count("PULocationID").as("tot_pick_up")).select(col("to_date_val"),date_format(col("to_date_val"), "EEEE").as("day_of_week"),col("tot_pick_up")).groupBy("day_of_week").agg((sum("tot_pick_up")/count("day_of_week")).as("avg_count")).orderBy($"avg_count".desc)

//.select(col("to_date"),date_format("to_date", "EEEEE"),col("tot_pick_up"))

//.groupBy("pickup_datetime_1").count().withColumnRenamed("pickup_datetime_1","pick_cnt")

//val df_part4a = df_part4.select(col("pickup_datetime_1"),col("to_date"),col("day_of_week"), to_date(col("day_of_week"),"yyyy-mm-dd").as("to_date"), to_timestamp(col("pickup_datetime")))

//.groupBy("day_of_week").agg(count("PULocationID")).as("pick_up")

//.agg(sum("PULocationID").as("pickup_cnt"))

//df_part4a.limit(3).show()

display(df_part4a.limit(2))



// COMMAND ----------

// PART 5: For each particular hour of a day (0 to 23, 0 being midnight) - in their order from 0 to 23, find the zone in Brooklyn borough with the LARGEST number of pickups. 
// Output Schema: hour_of_day int, zone string, max_count int

// Hint: You may need to use "Window" over hour of day, along with "group by" to find the MAXIMUM count of pickups

// ENTER THE CODE BELOW

//df.withColumn("hour", hour(col("input_timestamp")))



//val windowSpecAgg  = Window.partitionBy("hour_of_day")

//col = new Column("ts")
//col = col.desc()
//WindowSpec w = Window.partitionBy("col1", "col2").orderBy(col)

//df.filter(df("state") === "OH")

val windowSpec  = Window.partitionBy("hour_of_day").orderBy($"max_count".desc)

//.withColumn("max", max(col("salary")).over(windowSpecAgg))

//.agg(sum("number_activities").as("total_number_activities"))

val df_part5 = df_filter.join(df_taxi_zone,df_filter("PULocationID") === df_taxi_zone("LocationID")).filter(df_taxi_zone("Borough") === "Brooklyn").select(col("PULocationID"),col("pickup_datetime"),col("Borough"),col("Zone"),hour(col("pickup_datetime")).alias("hour_of_day")).groupBy("hour_of_day","Zone").count().withColumnRenamed("count","max_count").select(col("hour_of_day"),col("Zone"),col("max_count")).withColumn("row_number",row_number.over(windowSpec))

//.agg(count().as("max_count"))

//val df_part5a = df_part5.select(col("hour_of_day"),col("Zone"),col("max_count")).withColumn("row_number",row_number.over(windowSpec))

val df_part5b = df_part5.select(col("hour_of_day"),col("Zone"),col("max_count")).filter(df_part5("row_number") === 1).orderBy($"hour_of_day")

//.withColumn("row_number",row_number.over(windowSpec))


//,("row_number",row_number.over(windowSpec)))


//withColumn("hour", hour(col("pickup_datetime")))

//df_part5.printSchema
//df_part5a.show()
//df_part5a.printSchema
//df_part5b.show(24)
display(df_part5b)
//groupBy("Borough").agg(sum("number_activities").as("total_number_activities")).orderBy($"total_number_activities".desc).selectExpr("Borough","cast(total_number_activities as int) total_number_activities")


// COMMAND ----------

// PART 6 - Find which 3 different days of the January, in Manhattan, saw the largest percentage increment in pickups compared to previous day, in the order from largest increment % to smallest increment %. 
// Print the day of month along with the percent CHANGE (can be negative), rounded to 2 decimal places, in number of pickups compared to previous day.
// Output Schema: day int, percent_change float


// Hint: You might need to use lag function, over a window ordered by day of month.

// ENTER THE CODE BELOW

/*
val w = org.apache.spark.sql.expressions.Window.orderBy("day")  
val tmp_df = df_filter.filter("month(pickup_datetime) == 1").select($"PULocationID" as "LocationID", $"pickup_datetime")
.join(df_taxi_zone.filter("Borough == 'Manhattan'"),Seq("LocationID"))
.select(dayofmonth(col("pickup_datetime")) as "day").groupBy("day").count().orderBy("day").withColumn("last_day_count", lag("count", 1, 0).over(w))
.withColumn("percent_change", round((($"count" - $"last_day_count") / $"last_day_count") * 100, 2))
.filter("day != 1")
.orderBy($"percent_change".desc)
.drop("count","last_day_count")
.limit(3)
*/

//val winSpec = Window.partitionBy("month").orderBy("day")

val winSpec = org.apache.spark.sql.expressions.Window.orderBy("day")


val df_part6 = df_filter.filter("month(pickup_datetime) == 1").select($"PULocationID" as "LocationID", $"pickup_datetime")
.join(df_taxi_zone.filter("Borough == 'Manhattan'"),Seq("LocationID"))
.select(dayofmonth(col("pickup_datetime")) as "day").groupBy("day").count().orderBy("day").withColumn("last_day_count", lag("count", 1, 0).over(winSpec))
.withColumn("percent_change", round((($"count" - $"last_day_count") / $"last_day_count") * 100, 2))
.filter("day != 1")
.orderBy($"percent_change".desc)
.drop("count","last_day_count")
.limit(3)



display(df_part6.limit(3))

// COMMAND ----------


