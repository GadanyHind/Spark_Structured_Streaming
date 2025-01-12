package gadany.hind;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class App2 {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        SparkSession ss = SparkSession.builder()
                .master("local[*]")
                .appName("Spark Streaming")
                .getOrCreate();

        StructType schema = new StructType(new StructField[]{
               new StructField("Name", DataTypes.StringType,true, Metadata.empty()),
               new StructField("Price", DataTypes.DoubleType,true, Metadata.empty()),
               new StructField("Quantity", DataTypes.IntegerType,true, Metadata.empty()),
        });

        Dataset<Row> inputTable = ss.readStream()
                .option("header", "true")
                .schema(schema)
                .csv("hdfs://namenode/8020/input");

        StreamingQuery query=inputTable.writeStream()
                .outputMode("complete")
                .format("console")
                .start();
        query.awaitTermination();
    }
}
