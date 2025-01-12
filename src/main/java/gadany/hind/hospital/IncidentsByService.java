package gadany.hind.hospital;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class IncidentsByService {
    public static void main(String[] args) {
        SparkSession sparkSession=SparkSession.builder().appName("TP_SPARK_SQL_Exercise1")
                .master("local[*]").getOrCreate();

        Dataset<Row> dataset1=sparkSession.read()
                .option("header", "true")
                .option("multiline",true)
                .csv("incidents.csv");

        dataset1.createOrReplaceTempView("incidents");


        dataset1.groupBy("service").agg(functions.count("Id")
                .alias("NombreIncidents")).show(); //exe2
        }
}
