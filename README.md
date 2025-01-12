# README : Applications Spark pour l'analyse des incidents de l'hôpital

Ce projet contient deux applications Spark qui permettent d'analyser les incidents d'un hôpital à partir d'un fichier CSV. Chaque application répond à une question spécifique et est implémentée en Java.

---

## Structure des données

Le fichier `incidents.csv` doit avoir la structure suivante :

| **Colonne**  | **Description**                     |
|--------------|-------------------------------------|
| **Id**       | Identifiant unique de l'incident   |
| **titre**    | Titre de l'incident                |
| **description** | Description de l'incident         |
| **service**  | Service de l'hôpital impliqué      |
| **date**     | Date de l'incident (format `yyyy-MM-dd`) |

### Exemple de fichier `incidents.csv`

```csv
Id,titre,description,service,date
1,Incendie,Feu dans une salle,Service A,2023-01-10
2,Panne électrique,Coupure générale,Service B,2023-01-11
3,Incident médical,Erreur de dosage,Service C,2022-12-20
4,Inondation,Eau dans le sous-sol,Service A,2022-11-15
5,Panne électrique,Coupure mineure,Service B,2023-01-12
```

---

## Applications

### 1. **Afficher le nombre d'incidents par service**

Cette application regroupe les incidents par service et calcule le nombre total d'incidents pour chaque service.

#### Fonctionnement
1. Charge les données à partir du fichier CSV.
2. Regroupe les incidents par `service`.
3. Affiche le nombre total d'incidents pour chaque service.

#### Code
```java
package gadany.hind.hospital;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class IncidentsByService {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("IncidentsByService")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset1 = sparkSession.read()
                .option("header", "true")
                .option("multiline", true)
                .csv("incidents.csv");

        dataset1.groupBy("service").agg(functions.count("Id")
                .alias("NombreIncidents")).show();
    }
}
```

#### Résultat 

<img width="481" alt="Screenshot 2025-01-12 at 21 51 05" src="https://github.com/user-attachments/assets/974ad619-9586-4f1a-bc82-b878b653eefb" />

### 2. **Afficher les deux années avec le plus grand nombre d'incidents**

Cette application identifie les deux années où le plus grand nombre d'incidents a été enregistré.

#### Fonctionnement
1. Charge les données à partir du fichier CSV.
2. Ajoute une colonne contenant l'année extraite de la colonne `date`.
3. Regroupe les incidents par `Year` et compte le nombre total d'incidents pour chaque année.
4. Trie les résultats par ordre décroissant et affiche les deux années avec le plus grand nombre d'incidents.

#### Code
```java
package gadany.hind.hospital;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class TopYearsWithIncidents {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("TopYearsWithIncidents")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> incidentsDF = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("incidents.csv");

        incidentsDF = incidentsDF.withColumn("Year", year(col("date")));

        Dataset<Row> incidentsParAnnee = incidentsDF.groupBy("Year")
                .agg(count("Id").alias("NombreIncidents"))
                .orderBy(col("NombreIncidents").desc());

        incidentsParAnnee.show(2);
    }
}
```

#### Résultat 

<img width="481" alt="Screenshot 2025-01-12 at 21 51 35" src="https://github.com/user-attachments/assets/5c718368-344a-4d94-9105-8bd3fe4a3371" />


