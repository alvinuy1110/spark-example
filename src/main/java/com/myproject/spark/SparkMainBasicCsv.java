package com.myproject.spark;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.util.HashMap;
import java.util.Map;

import static org.apache.ivy.osgi.repo.AbstractOSGiResolver.RequirementStrategy.first;
import static org.apache.spark.sql.functions.*;

@Slf4j
public class SparkMainBasicCsv {

    public static void main(String[] args) throws Exception {
        SparkMainBasicCsv sparkMain = new SparkMainBasicCsv();

        SparkSession spark = sparkMain.getSparkSession();

        String jsonFile = "/home/user/Dataset/Toronto/Parking/Parking_Tags_Data_sample.csv";

        // large data set
        jsonFile= "/home/user/Dataset/Toronto/Parking/all.csv";

        Dataset<Row> df = sparkMain.loadCsv(spark, jsonFile);

      //  df.cache(); // to reuse the data without reload
        sparkMain.demoBasic(df);


        sparkMain.demoSQL(spark, df);

        Thread.sleep(10000000);
    }

    public void demoBasic(Dataset<Row> df) {
        log.info("Show json data");

        // show stats
        //df.describe().show();

        // Displays the content of the DataFrame to stdout
        df.show(5);
// Print the schema in a tree format
        df.printSchema();

        df.select("tagNumberMasked").show();


    }

    public void demoSQL(SparkSession spark, Dataset<Row> df) {
        //spark.catalog().clearCache();

        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("tickets");

        // cache the table

        Dataset<Row> sqlDF = spark.sql("SELECT tagNumberMasked FROM tickets");
        sqlDF.show(5);
        spark.catalog().cacheTable("tickets");


// very slow
//        Dataset<Row> sqlDF2 =sqlDF.withColumn("infDateYYYYMMDD",date_format(df.col("infractionDate"),"yyyy-MMM-dd"));
//
//        sqlDF2.createOrReplaceTempView("tickets2");
//        spark.catalog().cacheTable("tickets2");
//        sqlDF = spark.sql("SELECT infDateYYYYMMDD as infractionDate, count(1) " +
//                "FROM tickets2 " +
//                "group by infDateYYYYMMDD");

//SLOW  178 secs
        sqlDF = spark.sql("SELECT date_format(infractionDate,'yyyy-MMM-dd') as infractionDate, count(1) " +
                "FROM tickets " +
                " where infractionDate is not null " +
                "group by date_format(infractionDate,'yyyy-MMM-dd') order by 2 desc");

//        df.groupBy(date_format(col("infractionDate"),"yyyy-MMM-dd"))
//                .agg(count("infractionDate")).explain(true);
        // DF approach  188 secs
//        sqlDF =df.groupBy(date_format(col("infractionDate"),"yyyy-MMM-dd"))
//                .agg(count("infractionDate")).explain(true);

        log.info("Group by date");
        sqlDF.show(3);

        sqlDF = spark.sql("SELECT infractionCode, infractionDesc" +
                ", count(1) as TotalCount, sum(fineAmt) as TotalFine " +
                ", avg(fineAmt) as AvgFine, max(fineAmt) as MaxFine " +
                "FROM tickets " +
                "group by infractionCode, infractionDesc " +
                "order by 3 desc"
        );

        int limit = 10;
        log.info("Group by infraction code TOP " +limit);
        sqlDF.show(limit, false);


        sqlDF = spark.sql("SELECT location2" +
                ", count(1) as TotalCount, sum(fineAmt) as TotalFine " +
                ", avg(fineAmt) as AvgFine, max(fineAmt) as MaxFine " +
                "FROM tickets " +
                "group by location2 " +
                "order by 2 desc"
        );


        log.info("Group by location2 TOP " +limit);
        sqlDF.show(limit, false);

        sqlDF = spark.sql("SELECT tagNumberMasked" +
                ", count(1) as TotalCount, sum(fineAmt) as TotalFine " +
                ", avg(fineAmt) as AvgFine, max(fineAmt) as MaxFine " +
                "FROM tickets " +
                "group by tagNumberMasked " +
                "order by 2 desc"
        );


        log.info("Group by tagNumberMasked TOP " +limit);
        sqlDF.show(limit, false);
    }

    public SparkSession getSparkSession() {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                //.config("spark.some.config.option", "some-value")

                //see https://spark.apache.org/docs/latest/configuration.html
                .config("spark.master", "local[4]") // local spark, n-cores"


                .config("spark.memory.fraction", ".8")

//        spark.memory.fraction expresses the size of M as a fraction of the (JVM heap space - 300MB) (default 0.6). The rest of the space (40%) is reserved for user data structures, internal metadata in Spark, and safeguarding against OOM errors in the case of sparse and unusually large records.
//                spark.memory.storageFraction expresses the size of R as a fraction of M (default 0.5). R is the storage space within M where cached blocks immune to being evicted by execution.
//        The value of spark.memory.fraction should be set in order to fit this amount of heap space comfortably within the JVM’s old or “tenured” generation. See the discussion of advanced GC tuning below for details.
                .getOrCreate();

        return spark;
    }

    public Dataset<Row> loadCsv(SparkSession spark, String path) {

        // see https://github.com/databricks/spark-csv
        Map<String, String> options = new HashMap<>();
        options.put("header", "true");//reading the headers

        options.put("spark.sql.inMemoryColumnarStorage.batchSize", "20000");
        options.put("spark.sql.shuffle.partitions", "1000");
        options.put("spark.sql.inMemoryColumnarStorage.partitionPruning", "true");



        //options.put("dateFormat","YYYYMMDD");

        StructType customSchema = new StructType(new StructField[]{
                new StructField("tagNumberMasked", DataTypes.StringType, false, Metadata.empty()),
                new StructField("infractionDateAsStr", DataTypes.StringType, true, Metadata.empty()),
                new StructField("infractionCode", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("infractionDesc", DataTypes.StringType, true, Metadata.empty()),
                new StructField("fineAmt", DataTypes.DoubleType, true, Metadata.empty()),

                new StructField("infractionTimeAsStr", DataTypes.StringType, true, Metadata.empty()),
                new StructField("location1", DataTypes.StringType, true, Metadata.empty()),
                new StructField("location2", DataTypes.StringType, true, Metadata.empty()),
                new StructField("location3", DataTypes.StringType, true, Metadata.empty()),
                new StructField("location4", DataTypes.StringType, true, Metadata.empty()),
                new StructField("province", DataTypes.StringType, true, Metadata.empty())
        });

        Dataset<Row> df = spark.read().options(options).schema(customSchema)
                .csv(path);

//Define a udf to concatenate two passed in string values
        df = df.withColumn("infractionDate",
                (unix_timestamp(concat(df.col("infractionDateAsStr"),
                        lit(" "), df.col("infractionTimeAsStr"))
                        ,
                        "yyyyMMdd HHmm")).cast(DataTypes.TimestampType)

        );

        return df;

    }
}
