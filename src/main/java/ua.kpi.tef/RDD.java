package ua.kpi.tef;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Таня on 21.10.2018.
 */
public class RDD {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SQL Spark queries").setMaster("local");
        // create Spark Context
        SparkContext context = new SparkContext(conf);
        // create spark Session
        SparkSession sparkSession = new SparkSession(context);
       // RDD rdd = sparkSession.read().option("header", true).option("inferSchema", true).csv("D:/first.csv").rdd();
        JavaRDD<String> animalsRDD = sparkSession.sparkContext()
                .textFile("D:/first.txt", 1)
                .toJavaRDD();

        // The schema is encoded in a string
        String schemaString = "State Region Reference Animal_Type Animal_Name Breed_Description " +
                "Status_Description Suburb Year Gender_Desexed Colour Animal_Desexed Animal_Date_of_Birth " +
                "latitude longitude";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDD = animalsRDD.map((Function<String, Row>) record -> {
            String[] attributes = record.split(",");
            return RowFactory.create(attributes[0], attributes[1],
                    attributes[2], attributes[3],
                    attributes[4], attributes[5],
                    attributes[6], attributes[7],
                    attributes[8], attributes[9],
                    attributes[10], attributes[11],
                    attributes[12], attributes[13],attributes[14].trim());
        });

        // Apply the schema to the RDD
        Dataset<Row> animalsDataFrame = sparkSession.createDataFrame(rowRDD, schema);

        // Creates a temporary view using the DataFrame
        animalsDataFrame.createOrReplaceTempView("animals");
        Dataset<Row> results = sparkSession.sql("SELECT Animal_Name FROM animals");

        // The results of SQL queries are DataFrames and support all the normal RDD operations
        // The columns of a row in the result can be accessed by field index or by field name
        Dataset<String> namesDS = results.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                Encoders.STRING());
        namesDS.show();

        Dataset<Row> results1 = sparkSession.sql("SELECT Animal_Name FROM " +
                "animals WHERE Animal_Name = \"Max\" ");
        Dataset<String> ds = results1.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                Encoders.STRING());
        ds.show();

        Dataset<Row> results2 = sparkSession.sql("SELECT Breed_Description " +
                "FROM animals WHERE Breed_Description = \"PAPILLON\" ");
        Dataset<String> ds2 = results2.map(
                (MapFunction<Row, String>) row -> row.getString(0),
                Encoders.STRING());
        ds2.show();
    }
}
