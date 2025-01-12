package gadany.hind;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class App1 {
    public static void main(String[] args) {
        SparkSession ss= SparkSession.builder().
                appName("TP4 SPARK SQL").
                master("local[*]").
                getOrCreate();

        Dataset<Row> df1=ss.read().option("header",true).option("inferSchema",true).csv("product.csv");
        df1.createOrReplaceTempView("products");

        /*df1.printSchema();
        df1.show();

        df1.where("price>=20000").show();

        df1.where(col("price").gt(12000)).show();

        df1.groupBy(col("Name")).avg("price").show();

        df1.select(col("price"),col("Name")).show();

        df1.select(col("Name"),col("price")).orderBy(col("price").desc()).show();
        */

        ss.sql("select * from products where price>15000").show();
        ss.sql("select Name,avg(Price) from products group by Name").show();
    }
}
