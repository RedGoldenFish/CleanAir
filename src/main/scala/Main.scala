package main

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.SparkContext



object Main {
    def main(args: Array[String]) : Unit = {
        println("Time to get started")

        val spark: SparkSession = SparkSession.builder()
            .master("local[1]")
            .appName("CleanAir")
            .getOrCreate()


        parseGas(spark, "data/ExportMoy.annuelle-NO2-2019.csv", "outputs/tmp/NO2")
        parseGas(spark, "data/ExportMoy.annuelle-O3-2019.csv", "outputs/tmp/O3")
        parseGas(spark, "data/ExportMoy.annuelle-PM2.5-2019.csv", "outputs/tmp/PM2.5")
        parseGas(spark, "data/ExportMoy.annuelle-PM10-2019.csv", "outputs/tmp/PM10")
        parseGas(spark, "data/ExportMoy.annuelle-SO2-2019.csv", "outputs/tmp/SO2")

        return
    }

    def parseGas(spark: SparkSession, csvPath : String, outputPath: String) : Unit = {
        val gasHeaders = Seq("Date de début","Date de fin", "Organisme", 
        "code zas", "Zas", "code site",	"nom site",	"type d'implantation",	"Polluant",
        "type d'influence",	"discriminant", "Réglementaire", "type d'évaluation", "procédure de mesure",
        "type de valeur", "valeur", "valeur brute", "unité de mesure", "taux de saisie", "couverture temporelle",
        "couverture de données", "code qualité", "validité", "Latitude", "Longitude")

        val usefulHeaders= Seq("nom site",	"Polluant", "valeur brute", "Latitude", "Longitude")
        val df = spark.read.options(Map("delimiter" -> ";"))
            .option("header", true)
            .csv(csvPath)
            .select(usefulHeaders.head, usefulHeaders.tail : _*)
            .repartition(1)

        df.coalesce(1).write.mode(SaveMode.Overwrite)
                .option("header", true)
                .option("delimiter", ";")
                .csv(outputPath)
    }
}