import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, row_number, countDistinct, sum as spark_sum
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("DFB_Statistik").enableHiveSupport().getOrCreate()
oracle_url = "jdbc:oracle:thin:@oracle:1521/FREEPDB1"
oracle_user = os.environ.get("ORACLE_USER", "system")
oracle_password = os.environ["ORACLE_PASSWORD"]

# Stammdaten aus Oracle laden
spieler_df = spark.read.format("jdbc").option("url", oracle_url).option("dbtable", "SPIELER").option("user", oracle_user).option("password", oracle_password).option("driver", "oracle.jdbc.OracleDriver").load()
nationalmannschaft_df = spark.read.format("jdbc").option("url", oracle_url).option("dbtable", "NATIONALMANNSCHAFT").option("user", oracle_user).option("password", oracle_password).option("driver", "oracle.jdbc.OracleDriver").load()
finanztransaktionen_df = spark.read.format("jdbc").option("url", oracle_url).option("dbtable", "FINANZTRANSAKTIONEN").option("user", oracle_user).option("password", oracle_password).option("driver", "oracle.jdbc.OracleDriver").load()

# Bewegungsdaten aus CSV laden
spiel_df = spark.read.csv("/app/spiel.csv", header=True, inferSchema=True)
spielerbewertung_df = spark.read.csv("/app/spielerbewertung.csv", header=True, inferSchema=True)

# Bewertungen mit Spiel-Attributen (Saison, istLaenderspiel, Spieldatum) anreichern
ratings_with_match = spielerbewertung_df.join(spiel_df, on="Spiel_ID", how="inner")

# die Anzahl der Spiele insgesamt
anzahl_spiele = ratings_with_match.groupBy("Spieler_ID").agg(countDistinct("Spiel_ID").alias("Anzahl_Spiele"))

# nur offizielle Laenderspiele
anzahl_spiele_junior = ratings_with_match.filter(col("istLaenderspiel")=="Ja").groupBy("Spieler_ID").agg(countDistinct("Spiel_ID").alias("Anzahl_Laenderspiele"))

# ---Durchschnittliche Bewertung der Spiele insgesamt
avg_bewertung_gesamt = ratings_with_match.groupBy("Spieler_ID").agg(avg("Bewertung_in_Prozent").alias("AVG_Bewertung_Gesamt"))

# ---Durchschnittliche Bewertung der letzten 5 Spiele
window_spec = Window.partitionBy("Spieler_ID").orderBy(col("Spieldatum").desc())
ratings_ranked = ratings_with_match.withColumn("row_num", row_number().over(window_spec))
avg_bewertung_5spiele = ratings_ranked.filter(col("row_num")<=5).groupBy("Spieler_ID").agg(avg("Bewertung_in_Prozent").alias("AVG_Bewertung_5Spiele"))

# ---Durchschnittliche Bewertung der Spiele der aktuellen Saison
avg_bewertung_saison = ratings_with_match.filter(col("Saison")=="2025/2026").groupBy("Spieler_ID").agg(avg("Bewertung_in_Prozent").alias("AVG_Bewertung_Saison"))

# ---Prämienzahlung während der Nationalmannschaftskarriere
sum_praemie_nationalmannschaft = finanztransaktionen_df.groupBy("Spieler_ID").agg(spark_sum("Zahlungsbetrag").alias("SUM_Praemie_Nationalmannschaft"))

# ---Aktuell ausstehende Prämienzahlungen
ausstehende_praemien = finanztransaktionen_df.filter(col("Zahlungsstatus")=="ausstehend").groupBy("Spieler_ID").agg(spark_sum("Zahlungsbetrag").alias("ausstehende_Praemien"))

# Alle Kennzahlen an die Spielerliste joinen (left join, damit kein Spieler fehlt)
# Nationalmannschaftskategorie als Kontext-Spalte ergaenzt (aus der Nationalmannschaft-Tabelle)
spielerstatistik = spieler_df.select("Spieler_ID", "Name") \
    .join(nationalmannschaft_df, on="Spieler_ID", how="left") \
    .join(anzahl_spiele, on="Spieler_ID", how="left") \
    .join(anzahl_spiele_junior, on="Spieler_ID", how="left") \
    .join(avg_bewertung_gesamt, on="Spieler_ID", how="left") \
    .join(avg_bewertung_5spiele, on="Spieler_ID", how="left") \
    .join(avg_bewertung_saison, on="Spieler_ID", how="left")  \
    .join(sum_praemie_nationalmannschaft, on="Spieler_ID", how="left") \
    .join(ausstehende_praemien, on="Spieler_ID", how="left")

# Fehlende Zaehler/Summen auf 0 setzen (Durchschnitte bleiben NULL)
spielerstatistik = spielerstatistik.fillna(0, subset=[
    "Anzahl_Spiele", "Anzahl_Laenderspiele",
    "SUM_Praemie_Nationalmannschaft", "ausstehende_Praemien"
])

# Alte Tabelle mit abweichendem Schema entfernen
spark.sql("DROP TABLE IF EXISTS spielerstatistik")

spark.sql("""
CREATE TABLE IF NOT EXISTS spielerstatistik (
    Spieler_ID STRING,
    Name STRING,
    Nationalmannschaftskategorie STRING,
    Anzahl_Spiele BIGINT,
    Anzahl_Laenderspiele BIGINT,
    AVG_Bewertung_Gesamt DOUBLE,
    AVG_Bewertung_5Spiele DOUBLE,
    AVG_Bewertung_Saison DOUBLE,
    SUM_Praemie_Nationalmannschaft DOUBLE,
    ausstehende_Praemien DOUBLE
) USING PARQUET
""")


# In Hive schreiben
spielerstatistik.write.mode("overwrite").insertInto("spielerstatistik")

print("Spielerstatistik-Tabelle erfolgreich in Hive geschrieben.")
spark.sql("SELECT * FROM spielerstatistik").show(24)
