# README : Analyse des Incidents de l'Hôpital avec Spark Structured Streaming

Ce projet contient deux applications Spark utilisant Structured Streaming pour analyser les incidents d'un hôpital. Les incidents sont reçus en continu à partir de fichiers CSV et analysés en temps réel.

## Structure des Données

Le fichier CSV contenant les incidents doit avoir la structure suivante :

```
Id,titre,description,service,date
```

- **Id** : Identifiant unique de l'incident (ex. `1`).
- **titre** : Titre décrivant l'incident (ex. `Network Outage`).
- **description** : Description détaillée de l'incident (ex. `Experiencing network connectivity issues in Service A`).
- **service** : Service affecté par l'incident (ex. `IT`, `Security`, etc.).
- **date** : Date de l'incident au format `yyyy-MM-dd` (ex. `2023-01-15`).

### Exemple de fichier CSV :

```
Id,titre,description,service,date
1,Network Outage,Experiencing network connectivity issues in Service A,IT,2023-01-15
2,Server Downtime,Several servers in Service B are down,IT,2023-02-28
3,Data Loss,Critical data loss incident in Service A,Data Management,2023-03-12
4,Equipment Failure,Equipment failure in Service C affecting production,Production,2023-04-05
5,Security Breach,Security breach detected in Service B,Security,2023-05-20
```

## Applications Développées

### 1. Nombre d'Incidents par Service

Cette application regroupe les incidents par service et affiche en continu le nombre total d'incidents pour chaque service.

#### Fonctionnement :
1. Lecture des données du fichier CSV.
2. Groupement des incidents par service.
3. Agrégation pour compter le nombre d'incidents par service.
4. Affichage en temps réel des résultats.

#### Code Principal :
```java
Dataset<Row> dataset1 = sparkSession.read()
        .option("header", "true")
        .option("multiline", true)
        .csv("incidents.csv");

dataset1.groupBy("service")
        .agg(functions.count("Id").alias("NombreIncidents"))
        .show();
```

#### Résultat Exemple :

<img width="481" alt="Screenshot 2025-01-12 at 21 51 05" src="https://github.com/user-attachments/assets/974ad619-9586-4f1a-bc82-b878b653eefb" />

### 2. Années avec le Plus Grand Nombre d'Incidents

Cette application identifie les deux années ayant enregistré le plus grand nombre d'incidents.

#### Fonctionnement :
1. Lecture des données du fichier CSV.
2. Extraction de l'année à partir des dates des incidents.
3. Agrégation des incidents par année.
4. Tri des résultats par ordre décroissant.
5. Affichage des deux années avec le plus grand nombre d'incidents.

#### Code Principal :
```java
incidentsDF = incidentsDF.withColumn("Year", year(col("date")));

Dataset<Row> incidentsParAnnee = incidentsDF.groupBy("Year")
        .agg(count("Id").alias("NombreIncidents"))
        .orderBy(col("NombreIncidents").desc());

incidentsParAnnee.show(2);
```

#### Résultat Exemple :

<img width="481" alt="Screenshot 2025-01-12 at 21 51 35" src="https://github.com/user-attachments/assets/5c718368-344a-4d94-9105-8bd3fe4a3371" />
