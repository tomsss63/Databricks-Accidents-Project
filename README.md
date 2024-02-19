# Databricks-Accidents-Project

## Présentation du projet

Le but de ce projet est de voir les différentes étapes dans la création d'un classifieur permettant de prévoir la gravité d'un accident selon différents paramètres

## Les sources des données

Pour nous aider, j'ai pu m'appuyer sur la revue d'Ilyes Talbi ci-joint : https://larevueia.fr/xgboost-vs-random-forest-predire-la-gravite-dun-accident-de-la-route/

## Les étapes du projet

### 
Import des données et pré traitements

J'ai tout d'abord commencé par l'import des données en spark sur databricks, voici un exemple de la manière dont j'ai procédé :
```
# File location and type
file_location = "/FileStore/tables/vict.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ";"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)
```

Ensuite, l'objectif était d'importer le CSV dans le filestore (la base de données de notre projet DataBricks) :

```
# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "vict_csv"

df.write.format("parquet").saveAsTable(permanent_table_name)
```
Après, nous avons converti toutes nos tables spark en pandas afin de pouvoir les traiter + simplement et afin de les merger ensemble pour la création de nos classifieurs:

```
path = '/FileStore/tables/carac.csv'
dfspark = spark.read.csv(path, sep = ";", header=True)
carac = dfspark.toPandas()
print(carac.shape)
```

Après avoir réalisé toutes ces étapes, j'ai fusionné toutes les tables entre elles et j'ai converti le fichier fusionné en spark afin de pouvoir l'utiliser sur databricks et pour pouvoir l'inscrire dans le filestore de mon projet databricks :

```
# Fusionner les DataFrames
victime = vict.merge(veh, on=['Num_Acc', 'num_veh'])
accident = carac.merge(lieux, on='Num_Acc')
result_df = victime.merge(accident, on='Num_Acc')

result_df.to_csv("merged_data.csv", index=False)

# Convertissement en spark
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Create DataFrame").getOrCreate()
df = spark.createDataFrame(result_df)
df.show()

# Inscription du fichier fusionné dans le file store
permanent_table_name = "result_df"

df.write.format("parquet").saveAsTable(permanent_table_name)
```

## Preprocessing

Sur cette partie, je me suis complétement inspiré de la revue d'Ilyes Talbi. Le but étant de nettoyer les données afin de supprimer les valeurs vides ou encore d'encoder les variables catégoriques et les variables hrmm et de la position GPS.

## le modèle retenu et ses performances

J'ai retenu le modèle de la forêt aléatoire qui présentait les meilleurs résultats sur les données avec les paramètres suivants : {'max_depth': 10, 'min_samples_leaf': 2}. 
Accuracy sur l'ensemble de test : 0.62107




