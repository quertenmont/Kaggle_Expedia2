package lq

import java.lang._ 
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.TypeTag._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
           

import org.apache.spark.ml.classification.{OneVsRest, LogisticRegression}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.random._
import org.apache.spark.mllib.random.RandomRDDs


import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.feature._
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier 
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator 
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

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



object Expedia {

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


//  case class trainClass(date_time:String , site_name:Int , posa_continent:Int , user_location_country:Int , user_location_region:Int , user_location_city:Int , orig_destination_distance:Double , user_id:Int , is_mobile:Int , is_package:Int , channel:Int , srch_ci:String , srch_co:String , srch_adults_cnt:Int , srch_children_cnt:Int , srch_rm_cnt:Int , srch_destination_id:Int , srch_destination_type_id:Int , is_booking:Int , cnt:Int , hotel_continent:Int , hotel_country:Int , hotel_market:Int , hotel_cluster:Int)

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

        val sc = new SparkContext(new SparkConf().setAppName("Expedia"))
        sc.setLogLevel("WARN")

        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._


        val schema = StructType( Array(
           StructField("date_time", StringType, true),
           StructField("site_name", IntegerType, true),
           StructField("posa_continent", IntegerType, true),
           StructField("user_location_country", IntegerType, true),
           StructField("user_location_region", IntegerType, true),
           StructField("user_location_city", IntegerType, true),
           StructField("orig_destination_distance", DoubleType, true),
           StructField("user_id", IntegerType, true),
           StructField("is_mobile", IntegerType, true),
           StructField("is_package", IntegerType, true),
           StructField("channel", IntegerType, true),
           StructField("srch_ci", StringType, true),
           StructField("srch_co", StringType, true),
           StructField("srch_adults_cnt", IntegerType, true),
           StructField("srch_children_cnt", IntegerType, true),
           StructField("srch_rm_cnt", IntegerType, true),
           StructField("srch_destination_id", IntegerType, true),
           StructField("srch_destination_type_id", IntegerType, true),
           StructField("is_booking", IntegerType, true),
           StructField("cnt", IntegerType, true),
           StructField("hotel_continent", IntegerType, true),
           StructField("hotel_country", IntegerType, true),
           StructField("hotel_market", IntegerType, true),
           StructField("hotel_cluster", IntegerType, true)
        ))

 	//Loading the data into RDD with split
	val dataRowRDD =sc.textFile("file:///afs/cern.ch/user/q/querten/scratch0/train.csv.gz").map(_.split(",")).filter(! _(0).contains("date_time") ).map(a=>{
           var distance:Double = -1.0
           if(a(6).length>0)distance=a(6).toDouble
           Row(a(0),a(1).toInt,a(2).toInt,a(3).toInt,a(4).toInt,a(5).toInt,distance,a(7).toInt,a(8).toInt,a(9).toInt,a(10).toInt,a(11),a(12), a(13).toInt, a(14).toInt, a(15).toInt, a(16).toInt, a(17).toInt, a(18).toInt, a(19).toInt, a(20).toInt, a(21).toInt, a(22).toInt, a(23).toInt )
        }).sample(true, 0.0001)
        val trainDF = sqlContext.createDataFrame(dataRowRDD, schema)
           
        trainDF.registerTempTable("trainDF")
        sqlContext.udf.register("getNDays", getNDays _)

//        sqlContext.sql("SELECT srch_adults_cnt, srch_children_cnt, srch_rm_cnt, count(date_time) AS count FROM trainDF GROUP BY srch_adults_cnt, srch_children_cnt, srch_rm_cnt ORDER BY count DESC LIMIT 25").show(25)
//        sqlContext.sql("SELECT getNDays(srch_ci, srch_co) AS nday, count(date_time) AS count FROM trainDF GROUP BY getNDays(srch_ci, srch_co) ORDER BY count DESC LIMIT 25").show(25)
//        sqlContext.sql("SELECT user_location_country,user_location_region,user_location_city, count(date_time) AS count FROM trainDF GROUP BY user_location_country,user_location_region,user_location_city ORDER BY count DESC LIMIT 25").show(25)
//        sqlContext.sql("SELECT user_id, count(date_time) AS count FROM trainDF GROUP BY user_id ORDER BY count DESC LIMIT 25").show(25)


//        sqlContext.sql("SELECT hotel_cluster, AVG(srch_adults_cnt) as srch_adults_cnt, AVG(srch_children_cnt) as srch_children_cnt, AVG(srch_rm_cnt) as srch_rm_cnt, COUNT(date_time) as NEntry FROM trainDF GROUP BY hotel_cluster ORDER BY hotel_cluster ASC LIMIT 25").show(25)
//        sqlContext.sql("SELECT * FROM trainDF WHERE hotel_cluster=0 AND is_booking=1 LIMIT 50").show(50)


        val dataDF = sqlContext.sql("SELECT *, getNDays(srch_ci, srch_co) AS nday, CAST(hotel_cluster=99 AS DOUBLE) AS label0  FROM trainDF WHERE is_booking=1")


	val userLocationIndexer = new StringIndexer()
	  .setInputCol("user_location_country")
	  .setOutputCol("indexed_user_location")
	  .fit(dataDF)

        val userLocationEncoder = new OneHotEncoder()
          .setInputCol("indexed_user_location")
          .setOutputCol("encoded_user_location")

	val hotelLocationIndexer = new StringIndexer()
	  .setInputCol("hotel_continent")
	  .setOutputCol("indexed_hotel_location")
	  .fit(dataDF)

        val hotelLocationEncoder = new OneHotEncoder()
          .setInputCol("indexed_hotel_location")
          .setOutputCol("encoded_hotel_location")


	val assembler = new VectorAssembler()
	  .setInputCols(Array(
             "nday",
//             "srch_adults_cnt",
//             "srch_children_cnt",
             "srch_rm_cnt",
             "encoded_user_location",
             "encoded_hotel_location",
             "hotel_market"
          ))
	  .setOutputCol("features")

//	// Automatically identify categorical features, and index them.
//	val featureIndexer = new VectorIndexer()
//	  .setInputCol("features")
//	  .setOutputCol("indexedFeatures")
//	  .setMaxCategories(4) // features with > 4 distinct values are treated as continuous
//	  .fit(predataDF)

	// Index labels, adding metadata to the label column.
	// Fit on whole dataset to include all labels in index.
	val labelIndexer = new StringIndexer()
	  .setInputCol("label0")
	  .setOutputCol("indexedLabel")
	  .fit(dataDF)


	// Train a DecisionTree model.
	val dt = new DecisionTreeClassifier()
	  .setLabelCol("indexedLabel")
	  .setFeaturesCol("features")
          .setMaxDepth(10)
          .setMaxBins(1024)
          .setMinInstancesPerNode(1)
          .setMinInfoGain(0.0)
//          .setCacheNodeIds(true)
//          .setCheckpointInterval(10)

	val lr = new LogisticRegression()
	  .setMaxIter(10)
	  .setRegParam(0.3)
	  .setElasticNetParam(0.8)
   	  .setLabelCol("label0")
	  .setFeaturesCol("features")

	val layers = Array[Int](4, 5, 4, 100)
	// create the trainer and set its parameters
	val mpc = new MultilayerPerceptronClassifier()
	  .setLabelCol("indexedLabel")
	  .setFeaturesCol("features")
	  .setLayers(layers)
	  .setBlockSize(128)
	  .setSeed(1234L)
	  .setMaxIter(100)


	val rf = new RandomForestClassifier()
	  .setLabelCol("indexedLabel")
	  .setFeaturesCol("features")
	  .setNumTrees(2)
          .setMaxDepth(4)
          .setMaxBins(16)
          .setMinInstancesPerNode(1)
          .setMinInfoGain(0.001)
          .setCacheNodeIds(true)
          .setCheckpointInterval(10)

        val ovr = new OneVsRest()
        ovr.setClassifier(lr)
	ovr.setLabelCol("indexedLabel")
	ovr.setFeaturesCol("features")

	// Convert indexed labels back to original labels.
	val labelConverter = new IndexToString()
	  .setInputCol("prediction")
	  .setOutputCol("predictedLabel")
	  .setLabels(labelIndexer.labels)

	// Chain indexers and tree in a Pipeline
	val pipeline = new Pipeline().setStages(Array(userLocationIndexer, userLocationEncoder, hotelLocationIndexer, hotelLocationEncoder, assembler, labelIndexer , dt, labelConverter))


	// Split the data into training and test sets (30% held out for testing)
	val trainingDataArray = dataDF.randomSplit(Array.fill[scala.Double](2)(0.1))
        val models = trainingDataArray.map(trainingData => {
          val startTime = System.nanoTime()
          val model = pipeline.fit(trainingData.cache())
          val elapsedTime = (System.nanoTime() - startTime) / 1e9
          println(s"Training time: $elapsedTime seconds")
          model
        })

	// Train model.  This also runs the indexers.
//	val model = pipeline.fit(trainingData)

        // Make predictions.
	val predictions = models(0).transform(trainingDataArray(1))

	// Select example rows to display.
//	predictions.select("predictedLabel", "hotel_cluster", "features").show(5)
	predictions.show(25)
        println("----------")
//	predictions.filter("label0"=1).show(25)
        predictions.select("indexedLabel", "prediction").rdd.map(a => ((a(0), a(1)), 1)).reduceByKey(_+_).foreach(println)

//    fullPredictions.select("label", "labelIndex", "prediction", "probability").rdd.take(25).map(a => {
//      val Top5probs = a.getAs[DenseVector](3).toArray.zipWithIndex.sortWith(_._1 > _._1).take(5)
//      ((a.get(0), a.get(1), a.get(2)), Top5probs)
//    }).foreach(a => println(a._1.toString + " --> " + a._2.mkString(" | ")))








	// Select (prediction, true label) and compute test error
	val evaluator = new MulticlassClassificationEvaluator()
	  .setLabelCol("indexedLabel")
	  .setPredictionCol("prediction")
	  .setMetricName("precision")
	val accuracy = evaluator.evaluate(predictions)
	println("Test Error = " + (1.0 - accuracy))

//	val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
//	println("Learned classification forest model:\n" + rfModel.toDebugString)


        val getTop10 = udf((probability: DenseVector) => probability.toArray.zipWithIndex.sortWith(_._1 > _._1).take(10))
//        predictions.withColumn("Top5", col("probability").asInstanceOf[DenseVector].toArray.zipWithIndex.sortWith(_._1 > _._1).take(5) )
        predictions.withColumn("Top10", getTop10(col("probability")) ).select("features", "indexedLabel", "Top10").rdd.take(25).foreach(println)//foreach(a => println(a._1.mkString(" | ") + " --> " + a._2 + " --> " + a._3.mkString(" | ") ) )


    System.out.println("All Done")


  }
}
