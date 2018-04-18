package com.att.eeim

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import scala.sys.process._

object hack_ml {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    spark.conf.set("spark.app.name", "hack_ml")
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    spark.conf.set("spark.sql.pivotMaxValues","12000")
    sc.setLogLevel("ERROR")
    import spark.implicits._

    val hk00 = spark.read.json(s"/sandbox/sandbox8/data/hack2018/dev_data/features.csv_dev-dir/*/*.json")
    //val hk00 = spark.read.json(s"/sandbox/sandbox8/data/hack2018/eval_data/features.csv_eval-dir/*/*.json")
    val hk01 = hk00.select("etag","owner","description","presenter","rating","name","othercredits","subtitle","actor","duration","commercial","date")
    val hk02 = hk01.withColumn("commercial_count",size($"commercial")).drop("commercial")
    val hk03 = hk02.withColumn("hour",substring($"date",12,2))withColumn("month",substring($"date",6,2))

    //labels
    val lbl00 = spark.read.format("com.databricks.spark.csv").options(Map("path"->s"/sandbox/sandbox8/data/hack2018/dev_data/labels.csv.dev","header"->"true","delimiter"->"|")).load().na.fill("").na.replace("*",Map("null"->"","NULL"->"")).distinct()
    val gnr00 = spark.read.format("com.databricks.spark.csv").options(Map("path"->s"/sandbox/sandbox8/data/hack2018/dev_data/genres.expert.txt","header"->"false","delimiter"->"|")).load().na.fill("").na.replace("*",Map("null"->"","NULL"->"")).distinct().toDF("label")
    //val lbl00 = spark.read.format("com.databricks.spark.csv").options(Map("path"->s"/sandbox/sandbox8/data/hack2018/eval_data/features.csv.eval","header"->"true","delimiter"->"|")).load().na.fill("").na.replace("*",Map("null"->"","NULL"->"")).distinct()
    //val gnr00 = spark.read.format("com.databricks.spark.csv").options(Map("path"->s"/sandbox/sandbox8/data/hack2018/eval_data/genres.txt","header"->"false","delimiter"->"|")).load().na.fill("").na.replace("*",Map("null"->"","NULL"->"")).distinct().toDF("label")
    val gnr01 = gnr00.withColumn("lbl",lit("1"))

    //training
    val trn00 = lbl00.join(hk02,Seq("etag")).withColumn("genre",lower($"genre"))
    val trn01 = trn00.select("etag","genre").join(gnr01,$"genre".contains($"label"))//,"left_outer")
    val trn02 = trn01.groupBy("etag","genre").pivot("label").agg(first($"lbl")).na.fill("0")
    val trn03 = hk03.join(trn02,Seq("etag"))

    //feature02
    val ngrm00 = hk00.select("etag","sent.text").toDF("etag","text").withColumn("dialogue",$"text".cast("string")).drop("text").withColumn("dialogue",trim(regexp_replace(concat($"dialogue"),"\\^","-"))).withColumn("dialogue",trim(regexp_replace(concat($"dialogue"),"\\\n","")))
    //val ngrm01 = hk03.join(ngrm00,Seq("etag"))
    val ngrm01 = trn03.join(ngrm00,Seq("etag"))


    //write to hive
    val tbldata = ngrm01
    val table3 = "dev_full_set05"
    spark.sql("use hack2018")
    spark.sql(s"drop table $table3")
    val schema3 = tbldata.schema.map( x => x.name.concat(" ").concat( x.dataType.toString() match { case "StringType" => "STRING" case "LongType" => "STRING" case "IntegerType" => "STRING" case "ArrayType" => "STRING"} ) ).mkString(",")
    val hive_sql3 = s"CREATE EXTERNAL TABLE $table3 (" + schema3 + s")                  ROW FORMAT DELIMITED                 FIELDS TERMINATED BY '^'                LOCATION '/sandbox/sandbox8/data/hack2018/dev_data/$table3'"
    spark.sql(hive_sql3)
    tbldata.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header","false").option("delimiter","^").save(s"/sandbox/sandbox8/data/hack2018/dev_data/$table3")

    /*val trn02 = trn00.select("etag","named_entities").withColumn("temp",explode(split($"named_entities", "\\,"))).drop("named_entities")
    val trn03 = trn02.withColumn("temp1",split($"temp","\\:")).select(col("*")+:(0 until 2).map(i=>col("temp1").getItem(i).as(s"tmp$i")):_*).drop("temp","temp1")
    val trn04 = trn03.groupBy("etag").pivot("tmp0").agg(first("tmp1"))
      val trnnl1 = trn01.filter($"label".isNull).coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header","true").option("delimiter",",").save(s"/sandbox/sandbox8/data/hack2018/dev_data/nolabel/")
    val trnnl2 = spark.read.format("com.databricks.spark.csv").options(Map("path"->s"/sandbox/sandbox8/data/hack2018/dev_data/out.csv","header"->"true","delimiter"->",")).load().na.fill("").na.replace("*",Map("null"->"","NULL"->"")).distinct().withColumn("lbl",lit("1"))
    val trnnn = trn01.filter($"label".isNotNull)
    val trnfl = trnnn.union(trnnl2).na.replace("*",Map("sport"->"sports","special+B97"->"special"))
    //val trn02 = trnfl.groupBy("etag","genre").pivot("label").agg(first($"lbl")).na.fill("0")
 */


    sc.stop()
  }
}
