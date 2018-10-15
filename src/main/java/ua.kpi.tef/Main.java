package ua.kpi.tef;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;

/**
 * Created by Таня on 07.10.2018.
 */
public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SQL Spark queries").setMaster("local");
        // create Spark Context
        SparkContext context = new SparkContext(conf);
        // create spark Session
        SparkSession sparkSession = new SparkSession(context);

       // Dataset<Row> df = sparkSession.read().format("com.databricks.spark.csv").option("header", true).option("inferSchema", true).load("D:\first.csv");

        Dataset<Row> dfFirst = sparkSession.read().option("header", true).option("inferSchema", true).csv("D:/first.csv");
        Dataset<Row> dfSecond = sparkSession.read().option("header", true).option("inferSchema", true).csv("D:/second.csv");

        //("hdfs://localhost:9000/usr/local/hadoop_data/loan_100.csv");
        System.out.println("========== Print First Schema ============");
        dfFirst.printSchema();
        System.out.println("========== Print First Data ==============");
        dfFirst.show();

        System.out.println("========== Print Second Schema ============");
        dfSecond.printSchema();
        System.out.println("========== Print Second Data ==============");
        dfSecond.show();

        dfFirst.select(col("Animal_Name"),col("Animal_Type"),col("Gender_Desexed")).
                filter(col("Gender_Desexed").gt("Male")).show();
        dfFirst.select(col("Animal_Name"),col("Animal_Type"),col("Gender_Desexed")).
                join(dfSecond, dfFirst.col("Animal_Name").equalTo(dfSecond.col("Animal_Name"))).show();
        dfSecond.select(col("Animal_Name"),col("Animal_Type"),col("Gender"),
                col("Breed_Description_primary")).filter(col("Breed_Description_primary").gt("HUSKY")).show();

        //SQL Spark queries
        dfFirst.createOrReplaceTempView("first");
        dfSecond.createOrReplaceTempView("second");

        sparkSession.sql
                ("SELECT Animal_Name, Animal_Type, Gender_Desexed FROM first WHERE Gender_Desexed = \"Male\" ").show();
        sparkSession.sql
                ("SELECT* FROM first, second WHERE first.Animal_Name = second.Animal_Name").show();
        sparkSession.sql
                ("SELECT Animal_Name, Animal_Type, Gender, Breed_Description_primary FROM second " +
                        "WHERE Breed_Description_primary = \"HUSKY\" ").show();


/*        System.setProperty("hadoop.home.dir", "D:\\install\\spark-2.3.1-bin-hadoop2.7");
       *//* Create Spark Session to create connection to Spark *//*
        final SparkSession sparkSession = SparkSession.builder()
                .appName("Spark EntityResolution")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "./assets/")
                .getOrCreate();


        final DataFrameReader dataFrameReader = sparkSession
                .read()
                .option("header", "true")
                .option("inferSchema", "true");

        final Dataset<Row> df = dataFrameReader.csv("D:/first.csv");
        System.out.println("========== Print Schema ============");
        df.printSchema();
        System.out.println("========== Print Data ==============");
        df.show();*/

    }
}
