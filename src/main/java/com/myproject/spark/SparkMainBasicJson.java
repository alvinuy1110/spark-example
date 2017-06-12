package com.myproject.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

@Slf4j
public class SparkMainBasicJson {

    public static void main(String[] args) throws Exception {
        SparkMainBasicJson sparkMain = new SparkMainBasicJson();

        SparkSession spark = sparkMain.getSparkSession();

        String jsonFile = "src/main/resources/people.json";
        Dataset<Row> df = sparkMain.loadJson(spark, jsonFile);
        df.cache(); // to reuse the data without reload
        sparkMain.demoBasic(df);

        sparkMain.demoSQL(spark, df);
    }

    public void demoBasic(Dataset<Row> df) {
        log.info("Show json data");
        // Displays the content of the DataFrame to stdout
        df.show();
// Print the schema in a tree format
        df.printSchema();
// root
// |-- age: long (nullable = true)
// |-- name: string (nullable = true)

// Select only the "name" column
        df.select("name").show();
// +-------+
// |   name|
// +-------+
// |Michael|
// |   Andy|
// | Justin|
// +-------+

// Select everybody, but increment the age by 1
        df.select(col("name"), col("age").plus(1)).show();
// +-------+---------+
// |   name|(age + 1)|
// +-------+---------+
// |Michael|     null|
// |   Andy|       31|
// | Justin|       20|
// +-------+---------+

// Select people older than 21
        df.filter(col("age").gt(21)).show();
// +---+----+
// |age|name|
// +---+----+
// | 30|Andy|
// +---+----+

// Count people by age
        df.groupBy("age").count().show();
// +----+-----+
// | age|count|
// +----+-----+
// |  19|    1|
// |null|    1|
// |  30|    1|
// +----+-----+

    }

    public void demoSQL(SparkSession spark,Dataset<Row> df) {
        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("people");
        // cache the table
        spark.catalog().cacheTable("people");
        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.show();

        sqlDF = spark.sql("SELECT * FROM people where address.province ='ON'");
        sqlDF.show();
        log.info("Filtered by province");


        sqlDF = spark.sql("SELECT name, count(1) FROM people group by name");
        sqlDF.show();

        sqlDF = spark.sql("SELECT address.province as Province, count(1) Total , min(age) as MinAge, max(age) as MaxAge FROM people group by address.province");
        sqlDF.show();
    }

    public SparkSession getSparkSession() {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                //.config("spark.some.config.option", "some-value")

                //see https://spark.apache.org/docs/latest/configuration.html
                .config("spark.master", "local[4]") // local spark, n-cores
                .getOrCreate();

        return spark;
    }

    public Dataset<Row> loadJson(SparkSession spark, String path) {
        Dataset<Row> df = spark.read().json(path);

        return df;

    }
}
