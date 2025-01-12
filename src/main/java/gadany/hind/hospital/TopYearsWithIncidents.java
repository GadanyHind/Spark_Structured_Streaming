package gadany.hind.hospital;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class TopYearsWithIncidents {
    public static void main(String[] args) {
        // Créez une session Spark
        SparkSession spark = SparkSession.builder()
                .appName("TopYears")
                .master("local[*]")  // le mode de fonctionnement de Spark est (local dans cet example)
                .getOrCreate();

        // Chargez le fichier CSV en tant que DataFrame
        Dataset<Row> incidentsDF = spark.read()
                .option("header", true)
                .option("inferSchema",true)
                .csv("incidents.csv");

        // Ajoutez une colonne "Year" en extrayant l'année de la colonne "date"
        incidentsDF = incidentsDF.withColumn("Year", year(col("date")));

        // Effectuez une agrégation par année pour compter le nombre total d'incidents par année
        Dataset<Row> incidentsParAnnee = incidentsDF.groupBy("Year")
                .agg(count("Id").alias("NombreIncidents"));

        // Triez les résultats par ordre décroissant
        incidentsParAnnee = incidentsParAnnee.orderBy(col("NombreIncidents").desc());

        // Affichez les deux années avec le plus grand nombre d'incidents
        incidentsParAnnee.show(2); //exe3

    }
}
