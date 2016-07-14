package lq


//import scala._
import scala.Predef._
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.TypeTag._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineStage, Transformer}
import org.apache.spark.ml.util.MetadataUtils
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature.{VectorIndexer, StringIndexer}
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.random._
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.mllib.evaluation.{RegressionMetrics, MulticlassMetrics}

import java.lang._ 
import java.lang.Math
import java.util.Date;
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit


/*
date_time Timestamp string 
site_name ID of the Expedia point of sale (i.e. Expedia.com, Expedia.co.uk, Expedia.co.jp, ...) int 
posa_continent ID of continent associated with site_name int 
user_location_country The ID of the country the customer is located int 
user_location_region The ID of the region the customer is located int 
user_location_city The ID of the city the customer is located int 
orig_destination_distance Physical distance between a hotel and a customer at the time of search. A null means the distance could not be calculated double 
user_id ID of user int 
is_mobile 1 when a user connected from a mobile device, 0 otherwise tinyint 
is_package 1 if the click/booking was generated as a part of a package (i.e. combined with a flight), 0 otherwise int 
channel ID of a marketing channel int 
srch_ci Checkin date string 
srch_co Checkout date string 
srch_adults_cnt The number of adults specified in the hotel room int 
srch_children_cnt The number of (extra occupancy) children specified in the hotel room int 
srch_rm_cnt The number of hotel rooms specified in the search int 
srch_destination_id ID of the destination where the hotel search was performed int 
srch_destination_type_id Type of destination int 
is_booking 1 if a booking, 0 if a click tinyint 
cnt Numer of similar events in the context of the same user session bigint 
hotel_continent Hotel continent int 
hotel_country Hotel country int 
hotel_market Hotel market int 
hotel_cluster ID of a hotel cluster int 
*/



object ExpediaWithDF {

def getyear(s:String):String = {
   val year = s.substring(s.lastIndexOf('/')+1)
   year
}

def getNDays(ci:String, co:String):Int = {
   try{
      val formatter = new SimpleDateFormat("yyyy-MM-dd");
      var dateCI = formatter.parse(ci);
      var dateCO = formatter.parse(co);
      return TimeUnit.DAYS.convert(dateCO.getTime()-dateCI.getTime(), TimeUnit.MILLISECONDS).toInt
   }catch{
      case unknown : Throwable => return -99
   }
}

  case class trainClass(date_time:String , site_name:Int , posa_continent:Int , user_location:Int , orig_destination_distance:Double , user_id:Int , is_mobile:Int , is_package:Int , channel:Int , srch_ci:String , srch_co:String , srch_adults_cnt:Int , srch_children_cnt:Int , srch_rm_cnt:Int , srch_destination_id:Int , srch_destination_type_id:Int , is_booking:Int , cnt:Int , hotelLoc:Int , hotel_cluster:Int)

  //simple functio which prints all the stat
  def printStat(sqlContext:org.apache.spark.sql.SQLContext, input:org.apache.spark.sql.DataFrame, table:String){
      input.describe().show() //take quite some time
      sqlContext.sql("SELECT orig_destination_distance FROM " + table + " WHERE orig_destination_distance>=0").describe().show()
      //input.select(mean("posa_continent"), min("posa_continent"), max("posa_continent")).show()

  }

  def printCorrMatrix(sqlContext:org.apache.spark.sql.SQLContext, input:org.apache.spark.sql.DataFrame){
        val ColType = input.dtypes
        ColType.foreach(println)
        val NumCol = ArrayBuffer[String]()
        for(ct <- ColType){
           println(ct)
           if(! ct._2.contains("String")){
              NumCol+= ct._1 
           }           
        }
        NumCol.foreach(println)

        print("%20s".format("Header"))
        for(c1 <- NumCol){
           print(" | %20s".format(c1))
        }
        println(" |")
        for(c1 <- NumCol){
           print("%20s".format(c1))
           for(c2 <- NumCol){
              print("%+20.5f".format(input.stat.corr(c1, c2) ))
           }
           println(" |")
        }
   }


  def main(args: Array[String]) {

        val sc = new SparkContext(new SparkConf().setAppName("ExpediaWithDF"))
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._


 	//Loading the data into RDD with split
	val trainData =sc.textFile("file:///afs/cern.ch/user/q/querten/workspace/public/SparkTest/Data/train.csv").map(_.split(",")).filter(! _(0).contains("date_time") ).map(a=>{
           var distance:Double = -1.0
           if(a(6).length>0)distance=a(6).toDouble
//           trainClass(a(0),a(1).toInt,a(2).toInt,userLocClass(a(3).toInt,a(4).toInt,a(5).toInt),distance,a(7).toInt,a(8).toInt,a(9).toInt,a(10).toInt,a(11),a(12), a(13).toInt, a(14).toInt, a(15).toInt, a(16).toInt, a(17).toInt, a(18).toInt, a(19).toInt, hotelLocClass(a(20).toInt, a(21).toInt, a(22).toInt), a(23).toInt )
           trainClass(a(0),a(1).toInt,a(2).toInt,a(3).toInt,distance,a(7).toInt,a(8).toInt,a(9).toInt,a(10).toInt,a(11),a(12), a(13).toInt, a(14).toInt, a(15).toInt, a(16).toInt, a(17).toInt, a(18).toInt, a(19).toInt, a(21).toInt, a(23).toInt )
        }).sample(true, 0.1)

        val trainDF = trainData.toDF.cache()
        trainDF.registerTempTable("trainDF")
        //trainDF.show(50)
        //printStat(sqlContext, trainDF, "trainDF")
        sqlContext.udf.register("getNDays", getNDays _)

//        sqlContext.sql("SELECT srch_adults_cnt, srch_children_cnt, srch_rm_cnt, count(date_time) AS count FROM trainDF GROUP BY srch_adults_cnt, srch_children_cnt, srch_rm_cnt ORDER BY count DESC LIMIT 25").show(25)
//        sqlContext.sql("SELECT getNDays(srch_ci, srch_co) AS nday, count(date_time) AS count FROM trainDF GROUP BY getNDays(srch_ci, srch_co) ORDER BY count DESC LIMIT 25").show(25)
//        sqlContext.sql("SELECT user_location_country,user_location_region,user_location_city, count(date_time) AS count FROM trainDF GROUP BY user_location_country,user_location_region,user_location_city ORDER BY count DESC LIMIT 25").show(25)
//        sqlContext.sql("SELECT user_id, count(date_time) AS count FROM trainDF GROUP BY user_id ORDER BY count DESC LIMIT 25").show(25)


//        sqlContext.sql("SELECT hotel_cluster, AVG(srch_adults_cnt) as srch_adults_cnt, AVG(srch_children_cnt) as srch_children_cnt, AVG(srch_rm_cnt) as srch_rm_cnt, COUNT(date_time) as NEntry FROM trainDF GROUP BY hotel_cluster ORDER BY hotel_cluster ASC LIMIT 25").show(25)
//        sqlContext.sql("SELECT * FROM trainDF WHERE hotel_cluster=0 and is_booking=1 LIMIT 100").show(250)


//        printCorrMatrix(sqlContext, trainDF)


/*
        val simpleDataset = trainData.map(a => Vectors.dense(a.srch_adults_cnt, a.srch_children_cnt, a.srch_rm_cnt) ).randomSplit(Array(0.9,0.1))
        val trainDataset  = simpleDataset(0).cache()
        val testDataset   = simpleDataset(1).cache()


        // Cluster the data into two classes using KMeans
        val kmeans = new KMeans()
        kmeans.setK(25)
        kmeans.setMaxIterations(50)
        val clusters = kmeans.run(trainDataset)
        clusters.clusterCenters.foreach(println)

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        val WSSSE = clusters.computeCost(trainDataset)
        println("Within Set Sum of Squared Errors = " + WSSSE)

        // Save and load model
        //clusters.save(sc, "myModelPath")
        //val sameModel = KMeansModel.load(sc, "myModelPath")
*/





/*
// USING BDT
//FULL MVA with 99 labels
//    val selectStringBuf = ArrayBuffer[String]()
//    selectStringBuf += ("SELECT *, getNDays(srch_ci, srch_co) AS ndays ");
//    var clusterId = 0;
//    for( clusterId <- 1 until 100){
//       selectStringBuf += ", CAST(CASE WHEN hotel_cluster="+clusterId.toString()+" THEN 1.0 ELSE 0.0 END AS DOUBLE) as label"+clusterId.toString()
//    }
//    selectStringBuf += (" FROM trainDF WHERE is_booking=1");
//    println("selectString=" + selectStringBuf.mkString("").toString())

    val trainDFExtended = sqlContext.sql(selectStringBuf.mkString("").toString())
    val featureAssembler = new VectorAssembler()
           .setInputCols(Array("ndays", "posa_continent", "user_location_region", "orig_destination_distance", "is_mobile", "is_package", "channel", "srch_adults_cnt", "srch_children_cnt", "srch_rm_cnt", "srch_destination_id", "srch_destination_type_id", "hotel_country", "hotel_market"))
           .setOutputCol("features")
    val output = featureAssembler.transform(trainDFExtended)

    val splits = output.randomSplit(Array(0.9, 0.1))
    val training = splits(0)
    val test = splits(1)

    val DTR = new DecisionTreeRegressor()
          .setFeaturesCol("features")
          .setLabelCol("label1")
          .setMaxDepth(7)
          .setMaxBins(128)
          .setMinInstancesPerNode(1)
          .setMinInfoGain(0.0)
          .setCacheNodeIds(false)
          .setCheckpointInterval(10)


    // Fit the model
    val startTime = System.nanoTime()
    val treeModel = DTR.fit(training)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    println(treeModel) // Print model summary.
    println(treeModel.toDebugString) // Print full model.

    println("Evaluate on training sample:")
    evaluateClassificationModel(treeModel, training, "label1")
    println("Evaluate on test sample:")
    evaluateClassificationModel(treeModel, test, "label1")

    val fullPredictions = treeModel.transform(test).show(2500)
*/

/*
   
//USING RANDOM FOREST TO GET ALL PROBABILITIES AT ONCE
//FULL MVA with 1 label
    val selectStringBuf = ArrayBuffer[String]()
    selectStringBuf += ("SELECT *, getNDays(srch_ci, srch_co) AS ndays ");
    selectStringBuf += (", CAST((hotel_cluster) AS DOUBLE) as label")
    selectStringBuf += (" FROM trainDF WHERE is_booking=1");
    println("selectString=" + selectStringBuf.mkString("").toString())

 
    val trainDFExtended = sqlContext.sql(selectStringBuf.mkString("").toString())
    val featureAssembler = new VectorAssembler()
           .setInputCols(Array("ndays", "posa_continent", "user_location", "orig_destination_distance", "is_mobile", "is_package", "channel", "srch_adults_cnt", "srch_children_cnt", "srch_rm_cnt", "srch_destination_id", "srch_destination_type_id", "hotelLoc", "hotel_market"))
           .setOutputCol("features")


    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")
      .fit(trainDFExtended)

//    println("Indexer = " + indexer.toString())
//    println("Indexer Param : "); indexer.params.foreach(println)
//    println("Indexer Label : "); indexer.labels.foreach(println)


    val output = featureAssembler.transform(indexer.transform(trainDFExtended))



    val splits = output.randomSplit(Array(0.9, 0.1))
    val training = splits(0)
    val test = splits(1)

    val rf = new RandomForestClassifier()
          .setLabelCol("labelIndex")
          .setFeaturesCol("features")
          .setNumTrees(5)
          .setMaxDepth(7)
          .setMaxBins(128)
//          .setMinInstancesPerNode(1)
//          .setMinInfoGain(0.0)
//          .setCacheNodeIds(false)
//          .setCheckpointInterval(10)


    // Fit the model
    val startTime = System.nanoTime()
    val treeModel = rf.fit(training)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

//    println(treeModel) // Print model summary.
//    println(treeModel.toDebugString) // Print full model.
//   treeModel.save(sc, "file:////afs/cern.ch/user/q/querten/workspace/public/SparkTest/Expedia/RandomForestClassificationModel")

   // println("Evaluate on training sample:")
   // evaluateClassificationModel(treeModel, training, "label1")
   // println("Evaluate on test sample:")
   // evaluateClassificationModel(treeModel, test, "label1")

    val fullPredictions = treeModel.transform(test)
    fullPredictions.show(25)
//    fullPredictions.select("label", "labelIndex", "prediction", "probability").rdd.foreach(println)
    fullPredictions.select("label", "labelIndex", "prediction", "probability").rdd.take(25).map(a => {
      val Top5probs = a.getAs[DenseVector](3).toArray.zipWithIndex.sortWith(_._1 > _._1).take(5)
      ((a.get(0), a.get(1), a.get(2)), Top5probs)
    }).foreach(a => println(a._1.toString + " --> " + a._2.mkString(" | ")))
*/




//USING NEURAL NET  TO GET ALL PROBABILITIES AT ONCE
//FULL MVA with 1 label
    val selectStringBuf = ArrayBuffer[String]()
    selectStringBuf += ("SELECT *, getNDays(srch_ci, srch_co) AS ndays ");
    selectStringBuf += (", CAST((hotel_cluster) AS DOUBLE) as prelabel")
    selectStringBuf += (" FROM trainDF WHERE is_booking=1");
    println("selectString=" + selectStringBuf.mkString("").toString())
 
    val trainDFExtended = sqlContext.sql(selectStringBuf.mkString("").toString())
    val featureAssembler = new VectorAssembler()
           .setInputCols(Array("ndays", "posa_continent", "user_location", "orig_destination_distance", "is_mobile", "is_package", "channel", "srch_adults_cnt", "srch_children_cnt", "srch_rm_cnt", "srch_destination_id", "srch_destination_type_id", "hotelLoc"))
           .setOutputCol("features")

    val indexer = new StringIndexer()
      .setInputCol("prelabel")
      .setOutputCol("label")
      .fit(trainDFExtended)

    val output = featureAssembler.transform(indexer.transform(trainDFExtended))


    val splits = output.randomSplit(Array(0.9, 0.1))
    val training = splits(0).cache()
    val test = splits(1).cache()


    val layers = Array[Int](14, 100, 150, 1)
    val trainer = new MultilayerPerceptronClassifier()
       .setLayers(layers)
       .setBlockSize(128)
       .setSeed(1234L)
       .setMaxIter(100)
       .setLabelCol("label")
       .setFeaturesCol("features")


    // Fit the model
    val startTime = System.nanoTime()
    val model = trainer.fit(training)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

//    println(treeModel) // Print model summary.
//    println(treeModel.toDebugString) // Print full model.
//   treeModel.save(sc, "file:////afs/cern.ch/user/q/querten/workspace/public/SparkTest/Expedia/RandomForestClassificationModel")

   // println("Evaluate on training sample:")
   // evaluateClassificationModel(treeModel, training, "label1")
   // println("Evaluate on test sample:")
   // evaluateClassificationModel(treeModel, test, "label1")

   val fullPredictions = model.transform(test)

   val predictionAndLabels = fullPredictions.select("prediction", "label")
   val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("precision")


    fullPredictions.show(25)
//    fullPredictions.select("label", "labelIndex", "prediction", "probability").rdd.foreach(println)
    fullPredictions.select("label", "labelIndex", "prediction", "probability").rdd.take(25).map(a => {
      val Top5probs = a.getAs[DenseVector](3).toArray.zipWithIndex.sortWith(_._1 > _._1).take(5)
      ((a.get(0), a.get(1), a.get(2)), Top5probs)
    }).foreach(a => println(a._1.toString + " --> " + a._2.mkString(" | ")))








    sc.stop()
    System.out.println("All Done")
  }


  /**
   * Evaluate the given ClassificationModel on data.  Print the results.
   * @param model  Must fit ClassificationModel abstraction
   * @param data  DataFrame with "prediction" and labelColName columns
   * @param labelColName  Name of the labelCol parameter for the model
   *
   * TODO: Change model type to ClassificationModel once that API is public. SPARK-5995
   */
  private def evaluateClassificationModel(
      model: Transformer,
      data: DataFrame,
      labelColName: String): Unit = {
      val fullPredictions = model.transform(data)
      val predictions = fullPredictions.select("prediction").map(_.getDouble(0))
      val labels = fullPredictions.select(labelColName).map(_.getDouble(0))
      val RMSE = new RegressionMetrics(predictions.zip(labels)).rootMeanSquaredError
      println(s"  Root mean squared error (RMSE): $RMSE")
  }



}
