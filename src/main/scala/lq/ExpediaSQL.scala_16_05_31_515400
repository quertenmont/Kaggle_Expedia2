package lq

import java.lang._ 
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.TypeTag._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import Numeric.Implicits._
import Ordering.Implicits._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.hive.HiveContext

import org.apache.spark.ml.classification.{OneVsRest, LogisticRegression}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.random._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating


import org.apache.spark.ml._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.evaluation._

import java.lang.Math
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.{Calendar, Date}

import smile.classification._
import smile.data.{Attribute, NumericAttribute, NominalAttribute};

object ExpediaSQL {

   def getdateunit(s:String):Double = {
      if(s==""){
         scala.Double.NaN
      }else{
         val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
         var date = formatter.parse(s);
         val calendar = Calendar.getInstance();
         calendar.setTime(date);
         ((calendar.get(Calendar.DAY_OF_YEAR).toDouble / 365) + (calendar.get(Calendar.YEAR) - 2013).toDouble)/3.0;   //divide by 3 to be in [0-1] for date from 2013 till 2015
     }
   }


   def getyear(s:String):Double = {
      if(s==""){
         scala.Double.NaN
      }else{
         val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
         var date = formatter.parse(s);
         val calendar = Calendar.getInstance();
         calendar.setTime(date);
         (calendar.get(Calendar.YEAR) % 100).toDouble;
     }
   }

   def getmonth(s:String):Double = {
      if(s==""){
         scala.Double.NaN
      }else{
         val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
         var date = formatter.parse(s);
         val calendar = Calendar.getInstance();
         calendar.setTime(date);
         calendar.get(Calendar.MONTH ).toDouble;
      }
   }

   def getmonthunit(s:String):Double = {
      if(s==""){
         scala.Double.NaN
      }else{
         val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
         var date = formatter.parse(s);
         val calendar = Calendar.getInstance();
         calendar.setTime(date);
         calendar.get(Calendar.DAY_OF_YEAR).toDouble / 365;
      }
   }

   def getNDays(ci:String, co:String):Double = {
      if(ci=="" || co==""){
         scala.Double.NaN
      }else{
         val formatter = new SimpleDateFormat("yyyy-MM-dd");
         var dateCI = formatter.parse(ci);
         var dateCO = formatter.parse(co);
         return TimeUnit.DAYS.convert(dateCO.getTime()-dateCI.getTime(), TimeUnit.MILLISECONDS).toDouble
      }
   }

   def getmonthunit2(s:String):Double = {
      if(s==""){
         scala.Double.NaN
      }else{
         val formatter = new SimpleDateFormat("yyyy-MM-dd");
         var date = formatter.parse(s);
         val calendar = Calendar.getInstance();
         calendar.setTime(date);
         calendar.get(Calendar.DAY_OF_YEAR).toDouble / 365;
      }
   }

   def getfracioninweek(ci:String, co:String):Double = {
      if(ci=="" || co==""){
         scala.Double.NaN
      }else{
         val ndays = getNDays(ci, co).toInt
         val formatter = new SimpleDateFormat("yyyy-MM-dd");
         var date = formatter.parse(ci);
         val calendar = Calendar.getInstance();
         calendar.setTime(date);
         var ciDay = calendar.get(Calendar.DAY_OF_WEEK) -1  //0=Monday, Friday=4, 6=Sunday
         var NDaysInWeek=0
         for( i <- 0 to ndays ){
             if( ((ciDay+i)%7) < 4)NDaysInWeek+=1    //count friday has week end already
         }
         NDaysInWeek.toDouble / ndays
      }
   }


  case class user_location(country:Int , region:Int , city:Int)
  case class hotel_location(continent:Int , country:Int , market:Int);
  case class cico_date(ci:String , co:String)
  case class querry(id:Long, date_time:String , site_name:Int , posa_continent:Int , user:user_location, orig_destination_distance:scala.Double, user_id:Int , is_mobile:Int , is_package:Int , channel:Int , date:cico_date, srch_adults_cnt:Int , srch_children_cnt:Int , srch_rm_cnt:Int , srch_destination_id:Int , srch_destination_type_id:Int , is_booking:Int , cnt:Int , hotel:hotel_location, hotel_cluster:Int)
  case class result(t:Int, p:Seq[(Int,Double)])

  def printStat(results:org.apache.spark.rdd.RDD[(Long,result)]):Unit = {
    val statDestId =  results
       .map{ case(id,r) => (id, r.p.map{case (a,b) => a}) }
       .map(pair => Array[Boolean]( pair._2.size<=0,  pair._2.size==1,  pair._2.size==2,  pair._2.size==3,  pair._2.size==4,  pair._2.size==5, pair._2.size>5) )
       .map(r => r.map( b => if(b){1}else{0}) )
       .reduce( (a,b) => a.zip(b).map( pair => pair._1+pair._2) )

  }

  def Map5(EvaluatePerf:Boolean, label:String, results:org.apache.spark.rdd.RDD[(Long,result)], SampleSize:Option[Long]=None):Unit = {
       if(EvaluatePerf){
          Future {
          val out = results
          .map{ case(id,r) => Array[scala.Double](
             1, 
             r.p.map{case (a,b) => a}.distinct.take(5).view.zipWithIndex.map( p => if(p._1==r.t){1.toDouble/(p._2+1.0)}else{0.0} ).foldLeft(0.toDouble)(Math.max),
             if(r.p.size<=0){1}else{0},
             if(r.p.size==1){1}else{0},
             if(r.p.size==2){1}else{0},
             if(r.p.size==3){1}else{0},
             if(r.p.size==4){1}else{0},
             if(r.p.size==5){1}else{0},
             if(r.p.size> 5){1}else{0}
          )}
          .reduce( (a,b) => a.zip(b).map( pair => pair._1+pair._2) )

          if(SampleSize.isEmpty){
             println("Performance for %-15s with MAP@5 = %8f/%8f = %10.6f".format(label, out(1), out(0), out(1)/out(0)) )
          }else{
             println("Performance for %-15s with MAP@5 = %8f/%8f = %10.6f (%8f/%8f = %10.6f for filled entries)".format(label, out(1), SampleSize.get.toDouble, out(1)/SampleSize.get.toDouble, out(1), out(0), out(1)/out(0)) )
          }

          println("STAT:")
          println("   NEntries = " + out(0) )
          println("   with less than 5 Clusters = " + (out(0) - out(7) - out(8) ) )
          println("   with           0 Clusters = " + out(2) )
          println("   with           1 Clusters = " + out(3) )
          println("   with           2 Clusters = " + out(4) )
          println("   with           3 Clusters = " + out(5) )
          println("   with           4 Clusters = " + out(6) )
          println("   with           5 Clusters = " + out(7) )
          println("   with         > 5 Clusters = " + out(8) )
          Console.flush(); 

          }
       }
  }


  def getFeature(q:querry):Array[scala.Double] = { Array(
//     q.is_booking.toDouble,                                           //0.008960345628498184
     getfracioninweek(q.date_time, q.date.ci),                        //0.18652176538663412
     getdateunit(q.date_time).toDouble,                               //0.21222168148885226
     getmonthunit(q.date_time).toDouble,                              //0.12168157303697873
     getNDays(q.date_time, q.date.ci).toDouble / 500.0,               //0.11580736012338262
     getNDays(q.date.ci, q.date.co).toDouble / 10.0,                  //0.049839744582384465
//     q.is_mobile.toDouble,                                            //0.009771288917655701
//     q.is_package.toDouble,                                           //0.011969394406345533
       q.srch_adults_cnt.toDouble,
       q.srch_children_cnt.toDouble,
//     q.srch_adults_cnt.toDouble /q.srch_children_cnt.toDouble,        //0.013645113285712974
//     q.srch_rm_cnt.toDouble,                                          //0.00976872906573772
     //q.site_name.toDouble,                                         
//     q.posa_continent.toDouble,                                       //0.013339604126049788
     q.orig_destination_distance,                                     //0.20424295417203686
     math.min(q.channel.toDouble, 9.0)                                //0.04223044577973103
  )}

  def getFeatureAttributes():Array[Attribute] = { Array[Attribute](
//     new NumericAttribute("isBooking"),
     new NumericAttribute("fracWeek"),
     new NumericAttribute("date"),
     new NumericAttribute("month"),
     new NumericAttribute("bookDateDelta"),//     new NumericAttribute("ndays"),
     new NumericAttribute("ndays"),
//     new NumericAttribute("isMobile"),
//     new NumericAttribute("isPackage"),
     new NumericAttribute("nAdult"),
     new NumericAttribute("nChild"),
//     new NumericAttribute("fracKid"),
//     new NumericAttribute("nRoom"),
     //new NumericAttribute("siteName"),//, Array[String]("0", "1", "2", "3", "4")),
//     new NominalAttribute("continent", Array[String]("0", "1", "2", "3", "4")),
     new NumericAttribute("distance"),
     new NominalAttribute("channel", Array[String]("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"))
  )}

  def groupAndSave(EvaluatePerf:Boolean, save:Boolean, outputDir:String, groups:Array[org.apache.spark.rdd.RDD[(Long,result)]]):Unit = {
     val results = groups.reduceLeft{ (A,B) => 
        A.cogroup(B).map{ case(id, g) => (id, result(g._1.head.t, (if(g._2.size>0){g._1.head.p++g._2.head.p.take(10)}else{g._1.head.p})  )) }
     } 

     Map5(EvaluatePerf, outputDir, results)

     if(save){
        results
        .map{ case(id,r) => (id, r.p.map{case (a,b) => a}.distinct.take(5)) }
        .map{ case(id,p) => id.toString + "," + p.mkString(" ") }
        .coalesce(10)
        .saveAsTextFile("file:///afs/cern.ch/user/q/querten/scratch0/Expedia/" + outputDir);
     }
  }

  def groupAndSaveReorder(EvaluatePerf:Boolean, save:Boolean, outputDir:String, groups:Array[org.apache.spark.rdd.RDD[(Long,result)]]):Unit = {
     val results = groups.reduceLeft{ (A,B) => 
        A.cogroup(B).map{ case(id, g) => (id, result(g._1.head.t, (if(g._2.size>0){g._1.head.p++g._2.head.p.take(5)}else{g._1.head.p})  )) }          
     }
    
     val results2 = results.mapValues{ r => result(r.t, r.p.groupBy(_._1).mapValues{ listKeyValuePairs => (listKeyValuePairs.map(_._2).reduce{_+_} ) }.toSeq.sortWith{ case(a,b) =>  a._2 > b._2 } ) }
     Map5(EvaluatePerf, outputDir, results2)

     if(save &&  !EvaluatePerf){
        results2
        .map{ case(id,r) => (id, r.p.map{case (a,b) => a}.distinct.take(5)) }
        .map{ case(id,p) => id.toString + "," + p.mkString(" ") }
        .coalesce(10)
        .saveAsTextFile("file:///afs/cern.ch/user/q/querten/scratch0/Expedia/" + outputDir);
     }
  }

  def append_0(q:querry):java.lang.Double = { (getyear(q.date_time) - 12) + getmonthunit(q.date_time) }  //CHANGE wrt camilo
  def append_1(q:querry):java.lang.Double = { math.pow(append_0(q),3) * (6 + 19.0* q.is_booking.toDouble) } 
  def append_2(q:querry):java.lang.Double = { (2 + (8.0 * q.is_booking.toDouble)) }


  def main(args: Array[String]) {
        val conf = new SparkConf()
        conf.setAppName("ExpediaSQL")
        conf.set("spark.driver.memory", "4g")
        conf.set("spark.executor.memory", "2g")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //to avoid serialization issues with smile
//        conf.set("spark.kryoserializer.buffer.mb", "256"); 
        conf.set("spark.kryoserializer.buffer.max", "256mb");
        val sc = new SparkContext(conf)
        sc.setLogLevel("WARN")

        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._

        //OPTIONS
        val EvaluatePerf = false;
        val useMVA       = false;  //CHANGE wrt camilo
        val CamiloMPA5   = false

        var trainFile = "file:///afs/cern.ch/user/q/querten/workspace/public/SparkTest/Data/train.csv"
        var testFile  = "file:///afs/cern.ch/user/q/querten/scratch0/test.csv"

        if(CamiloMPA5){
            trainFile = "file:///afs/cern.ch/user/q/querten/workspace/public/SparkTest/check_score/train20132014I.csv"
            testFile  = "file:///afs/cern.ch/user/q/querten/workspace/public/SparkTest/check_score/test_2014II_isbooking.csv"
        }

 	//Loading the train data into RDD with split
	val inputTrainDataRowRDD =sc.textFile(trainFile, 500).filter(! _.contains("date_time") ).map(_.split(",")).zipWithUniqueId().map{ case(q,index) => 
           var distance:Double = {if(q(6).length>0){q(6).toDouble}else{-1}}// (q(6).toDouble*1E5).toInt}else{-1} }
           querry(-1*index, q(0),q(1).toInt,q(2).toInt, user_location(q(3).toInt,q(4).toInt,q(5).toInt),distance,q(7).toInt,q(8).toInt,q(9).toInt,q(10).toInt, cico_date(q(11),q(12)),
                  q(13).toInt, q(14).toInt, q(15).toInt, q(16).toInt, q(17).toInt, q(18).toInt, q(19).toInt, hotel_location(q(20).toInt, q(21).toInt, q(22).toInt), q(23).toInt )
        }

 	//Loading the test data into RDD with split
	val inputTestDataRowRDD =sc.textFile(testFile, 250).filter(! _.contains("date_time") ).map(_.split(",")).map{ q =>
           var distance:scala.Double = {if(q(7).length>0){q(7).toDouble}else{-1}}//{(q(7).toDouble*1E5).toInt}else{-1} }
           querry(q(0).toLong, q(1),q(2).toInt,q(3).toInt,user_location(q(4).toInt,q(5).toInt,q(6).toInt),distance,q(8).toInt,q(9).toInt,q(10).toInt,q(11).toInt,cico_date(q(12),q(13)), 
                  q(14).toInt, q(15).toInt, q(16).toInt, q(17).toInt, q(18).toInt, 1, 1, hotel_location(q(19).toInt, q(20).toInt, q(21).toInt), -1 )
        }

        
        //define the training and test samples that we are going to use
        val Array(trainDataRowRDD, testDataRowRDD) = {
           val toReturn = {
           if(EvaluatePerf && !CamiloMPA5){
              val training = inputTrainDataRowRDD.filter(q => getyear(q.date_time)<14 || getmonthunit(q.date_time)<0.5)
              val testing  = inputTrainDataRowRDD.filter(q => getyear(q.date_time)==14 && getmonthunit(q.date_time)>0.5 && q.is_booking==1 && q.cnt==1 )
              val trainingUsers = sc.broadcast( training.map(q => q.user_id).distinct.collect() )
              Array( training, testing.filter(q => trainingUsers.value.contains(q.user_id) ) )  //keep only testing corresponding to user in the training set
           }else if(EvaluatePerf){
              val TestTruth = sc.textFile("file:///afs/cern.ch/user/q/querten/workspace/public/SparkTest/check_score/int_hc_loic.txt").filter(! _.contains("hotel_cluster")).map(_.split(" ")).map{ IdandTruth => (IdandTruth(0).toLong, IdandTruth(1).toInt)}
              Array(
                 inputTrainDataRowRDD, 
                 inputTestDataRowRDD.map(q => (q.id.toLong, q)).cogroup(TestTruth).flatMap{case (key, values) => 
                    values._1.map(q => q.copy(hotel_cluster=values._2.head))  //add back the truth to the same data structure
                 }
              )
           }else{
              Array(inputTrainDataRowRDD, inputTestDataRowRDD )
           }
           }.map(_.cache())
           toReturn.map(rdd => Future { rdd.count()  }).foreach(f => Await.ready(f, 10000 minutes)) //run an action to trigger the processing of the samples in parallel
           toReturn
        }

        //Init : DEFINE THE OUTPUT FORMAT AND USE IT EVERYWHERE  (ALLOWS to comment some part of the code without hurts)
        val totalstartTime = System.nanoTime()
        val TEST = testDataRowRDD;
        val TESTsize = TEST.count

        //println("site_name = " + TEST.map(q => q.site_name.toDouble ).stats)


/////////////////////////////////////////////////////////////////////////////// 
///     INITIALIZATION 
///////////////////////////////////////////////////////////////////////////////  

        //get the most frequent clusters overall  (will be use for sorting in case of equality to avoid randomness in equality)
        val clusterFrequency =  trainDataRowRDD.map(q => ( q.hotel_cluster, append_0(q) ) ).reduceByKey(_+_).sortBy(_._2, false)  //Orered Sequence of (Cluster,Rate)
        val clusterFrequencyBD=  sc.broadcast( clusterFrequency.collect.toMap ) //broadcast the variable for performance optimization

/////////////////////////////////////////////////////////////////////////////// 
///     A:  MAKE A FIRST PREDICTION BASED ON THE DATA LEAK WITH MVA
///////////////////////////////////////////////////////////////////////////////  

        //Parse the train data and check if they are matches
        val resultsAMap = TEST
        .filter(q => q.orig_destination_distance >=0 ) 
        .map(q => ( (q.user.city, q.orig_destination_distance), q)  )
        .cogroup( trainDataRowRDD.filter{q => (q.orig_destination_distance >=0) }.map{q => ( (q.user.city, q.orig_destination_distance) , q)}    )
        .filter{ case(key,value) => ((value._1.size>0) && (value._2.size>0)) }
        .flatMap{ case(key, value) => 
           var test = value._1 //matching querry data
           val train = value._2 //matching hotel location mapping best clusters

           val hotelClustersOrdered = train.map{ q => (q.hotel_cluster, append_1(q)) }.groupBy(_._1).map{ case(key,listKeyValuePairs) => (key, listKeyValuePairs.map(_._2).reduce(_+_))   }  //get Map of cluster -> counts
                                   .toSeq.sortWith{ case(a,b) =>  if( a._2 != b._2  ){a._2 > b._2}else{ clusterFrequencyBD.value(a._1) > clusterFrequencyBD.value(b._1) }  }                //order the list
           val hotelClustersMap     = hotelClustersOrdered.map((a=>a._1)).take(5).zipWithIndex.toMap  //only keep the 2 most likely clusters


            if(hotelClustersOrdered.size>1 && train.count(q => true)>25){
              val attributes = getFeatureAttributes() 
              val labels:Array[scala.Int] = train.filter(q=> hotelClustersMap.contains(q.hotel_cluster)).map(q=> hotelClustersMap(q.hotel_cluster).toInt ).toArray
              val features:Array[Array[scala.Double] ] = train.filter(q=> hotelClustersMap.contains(q.hotel_cluster)).map(q => getFeature(q) ).toArray
              val classifier = new AdaBoost(attributes,  features.toArray, labels.toArray, 250, 2*hotelClustersMap.size)
              test.map{q =>
                     val feature   = getFeature(q)
                     var posterior = Array.fill[scala.Double]( hotelClustersMap.size)(0.0)
                     classifier.predict(feature , posterior)
                   val prediction = posterior.toSeq.zipWithIndex.sortWith( (a,b) => a._1 * hotelClustersOrdered(a._2)._2 > b._1 * hotelClustersOrdered(b._2)._2).map(pair => hotelClustersOrdered(pair._2))
                     (q.id, result(q.hotel_cluster, (prediction ++ hotelClustersOrdered).distinct.take(10)), null)
              }
           }else{
              test.map{q => (q.id, result(q.hotel_cluster, hotelClustersOrdered.take(10)), null  ) }
           }
        }
        val resultsA = resultsAMap.map(a => (a._1, a._2) ).cache()


/////////////////////////////////////////////////////////////////////////////// 
/// TESTA:  MAKE A FIRST PREDICTION BASED ON THE DATA LEAK WITH MVA
///////////////////////////////////////////////////////////////////////////////  

        //Parse the train data and check if they are matches
        val resultsTestAMap = TEST
        .filter(q => q.orig_destination_distance >=0 ) 
        .map(q => ( (q.user.city, q.orig_destination_distance), q)  )
        .cogroup( trainDataRowRDD.filter{q => (q.orig_destination_distance >=0) }.map{q => ( (q.user.city, q.orig_destination_distance) , q)}    )
        .filter{ case(key,value) => ((value._1.size>0) && (value._2.size>0)) }
        .flatMap{ case(key, value) => 
           var test = value._1 //matching querry data
           val train = value._2 //matching hotel location mapping best clusters

           val hotelClustersOrdered = train.map{ q => (q.hotel_cluster, append_1(q)) }.groupBy(_._1).map{ case(key,listKeyValuePairs) => (key, listKeyValuePairs.map(_._2).reduce(_+_))   }  //get Map of cluster -> counts
                                   .toSeq.sortWith{ case(a,b) =>  if( a._2 != b._2  ){a._2 > b._2}else{ clusterFrequencyBD.value(a._1) > clusterFrequencyBD.value(b._1) }  }                //order the list
           val hotelClustersMap     = hotelClustersOrdered.map((a=>a._1)).zipWithIndex.toMap  //only keep the 2 most likely clusters


            if(hotelClustersOrdered.size>1 && train.count(q => true)>10){
              test.map{teq =>

                     val trainCleaned = train.filter(q=> hotelClustersMap.contains(q.hotel_cluster))
                                        .toSeq.sortWith{ (q1,q2) => math.abs(getNDays(q1.date_time, teq.date_time)) < math.abs(getNDays(q2.date_time, teq.date_time))  }.take(50)


                     val hotelClustersOrderedB = trainCleaned.map{ q => (q.hotel_cluster, append_1(q)) }.groupBy(_._1).map{ case(key,listKeyValuePairs) => (key, listKeyValuePairs.map(_._2).reduce(_+_))   }  //get Map of cluster -> counts
                                    .toSeq.sortWith{ case(a,b) =>  if( a._2 != b._2  ){a._2 > b._2}else{ clusterFrequencyBD.value(a._1) > clusterFrequencyBD.value(b._1) }  }                //order the list
                     val hotelClustersMapB     = hotelClustersOrderedB.map((a=>a._1)).zipWithIndex.toMap  //only keep the 2 most likely clusters

                     if(hotelClustersOrderedB.size>1){

                     val attributes = getFeatureAttributes()// ++ Array(new NumericAttribute("timeProx"))
                     val labels:Array[scala.Int] = trainCleaned.map(q=> hotelClustersMapB(q.hotel_cluster).toInt ).toArray
                     val features:Array[Array[scala.Double] ] = trainCleaned.map(q => getFeature(q) ).toArray  //++ Array[scala.Double](math.abs(getNDays(q.date_time, teq.date_time))) ).toArray
                     val classifier = new AdaBoost(attributes,  features.toArray, labels.toArray, 250, 2*hotelClustersMapB.size)


                     val feature   = getFeature(teq) ++ Array[scala.Double](math.abs(getNDays(teq.date_time, teq.date_time)))
                     var posterior = Array.fill[scala.Double]( hotelClustersMapB.size)(0.0)
                     classifier.predict(feature , posterior)
                   val prediction = posterior.toSeq.zipWithIndex.sortWith( (a,b) => a._1 * hotelClustersOrderedB(a._2)._2 > b._1 * hotelClustersOrderedB(b._2)._2).map(pair => hotelClustersOrderedB(pair._2))
                     (teq.id, result(teq.hotel_cluster, (prediction ++ hotelClustersOrderedB).distinct.take(10)), null)
                     }else{
                        (teq.id, result(teq.hotel_cluster, hotelClustersOrderedB), null  )
                     }
              }
           }else{
              test.map{q => (q.id, result(q.hotel_cluster, hotelClustersOrdered.take(10)), null  ) }
           }
        }
        val resultsTestA = resultsTestAMap.map(a => (a._1, a._2) ).cache()




/////////////////////////////////////////////////////////////////////////////// 
///     B:  MAKE A PREDICTION BASED ON PAST HOTEL VISITED BY THE SAME USER 
///////////////////////////////////////////////////////////////////////////////  



        val resultsBMap = TEST
        .map(q => ( (q.user_id, q.srch_destination_id, q.hotel.country, q.hotel.market), q)  )
        .cogroup(trainDataRowRDD.map{q => ((q.user_id, q.srch_destination_id, q.hotel.country, q.hotel.market), q)} )    //That's a huge shuffle !!WARNING to reduce the time repartition to 1k
        .filter{ case(key,value) => ((value._1.size>0) && (value._2.size>0)) }
        .flatMap{ case(key, value) => 
           var test = value._1 //matching querry data
           val train = value._2 //matching hotel location mapping best clusters

           val hotelClustersOrderedLeak = train.filter(q=>q.orig_destination_distance>=0).map{ q => (q.hotel_cluster, append_0(q)) }.groupBy(_._1).map{ case(key,listKeyValuePairs) => (key, listKeyValuePairs.map(_._2).reduce(_+_))   }  //get Map of cluster -> counts
                                   .toSeq.sortWith{ case(a,b) =>  if( a._2 != b._2  ){a._2 > b._2}else{ clusterFrequencyBD.value(a._1) > clusterFrequencyBD.value(b._1) }  }                //order the list



              test.map{teq =>
                 var listA = Seq[(Int,Double)]()
                 var listB = Seq[(Int,Double)]()

                 val hotelClustersOrderedCityLeak = train.filter(q => q.user.city==teq.user.city && q.orig_destination_distance>=0).map{ q => (q.hotel_cluster, append_0(q)) }.groupBy(_._1).map{ case(key,listKeyValuePairs) => (key, listKeyValuePairs.map(_._2).reduce(_+_))   }  //get Map of cluster -> counts
                                            .toSeq.sortWith{ case(a,b) =>  if( a._2 != b._2  ){a._2 > b._2}else{ clusterFrequencyBD.value(a._1) > clusterFrequencyBD.value(b._1) }  }                //order the list

                 val hotelClustersOrderedCityNoLeak = train.filter(q => q.user.city==teq.user.city && q.orig_destination_distance<0).map{ q => (q.hotel_cluster, append_0(q)) }.groupBy(_._1).map{ case(key,listKeyValuePairs) => (key, listKeyValuePairs.map(_._2).reduce(_+_))   }  //get Map of cluster -> counts
                                            .toSeq.sortWith{ case(a,b) =>  if( a._2 != b._2  ){a._2 > b._2}else{ clusterFrequencyBD.value(a._1) > clusterFrequencyBD.value(b._1) }  }                //order the list


                 if(teq.orig_destination_distance<0){
                     listA = hotelClustersOrderedCityNoLeak.take(10)

                     if(listA.size>1 && train.count(q => true)>10){  //changed from 50
                        val hotelClustersMap     = listA.map((a=>a._1)).take(5).zipWithIndex.toMap  //only keep the 2 most likely clusters

                        val attributes = getFeatureAttributes() 
                        val labels:Array[scala.Int] = train.filter(q=> hotelClustersMap.contains(q.hotel_cluster)).map(q=> hotelClustersMap(q.hotel_cluster).toInt ).toArray
                        val features:Array[Array[scala.Double] ] = train.filter(q=> hotelClustersMap.contains(q.hotel_cluster)).map(q => getFeature(q) ).toArray
                        val classifier = new AdaBoost(attributes,  features.toArray, labels.toArray, 500, 4)

                        val feature   = getFeature(teq)
                        var posterior = Array.fill[scala.Double]( hotelClustersMap.size)(0.0)
                        classifier.predict(feature , posterior)
                        val prediction = posterior.toSeq.zipWithIndex.sortWith( (a,b) => a._1 * hotelClustersOrderedCityNoLeak(a._2)._2 > b._1 * hotelClustersOrderedCityNoLeak(b._2)._2).map(pair => hotelClustersOrderedCityNoLeak(pair._2))
                        listA = (listA++prediction).distinct
                     }

                 }


                 if(hotelClustersOrderedLeak.size>0 && hotelClustersOrderedCityLeak.size<=0){
                    listB = hotelClustersOrderedLeak.take(10)

                     if(listB.size>1 && train.count(q => true)>10){  //changed from 50
                        val hotelClustersMap     = listB.map((a=>a._1)).take(5).zipWithIndex.toMap  //only keep the 2 most likely clusters

                        val attributes = getFeatureAttributes() 
                        val labels:Array[scala.Int] = train.filter(q=> hotelClustersMap.contains(q.hotel_cluster)).map(q=> hotelClustersMap(q.hotel_cluster).toInt ).toArray
                        val features:Array[Array[scala.Double] ] = train.filter(q=> hotelClustersMap.contains(q.hotel_cluster)).map(q => getFeature(q) ).toArray
                        val classifier = new AdaBoost(attributes,  features.toArray, labels.toArray, 500, 4)

                        val feature   = getFeature(teq)
                        var posterior = Array.fill[scala.Double]( hotelClustersMap.size)(0.0)
                        classifier.predict(feature , posterior)
                        val prediction = posterior.toSeq.zipWithIndex.sortWith( (a,b) => a._1 * hotelClustersOrderedLeak(a._2)._2 > b._1 * hotelClustersOrderedLeak(b._2)._2).map(pair => hotelClustersOrderedLeak(pair._2))
                        listB = (listB++prediction).distinct
                     }


                 }

                 (teq.id, result(teq.hotel_cluster, (listA++listB).take(10)), null  )
           }
        }
              
        val resultsB = resultsBMap.map(a => (a._1, a._2) ).cache()
//        Map5(EvaluatePerf,  "User,Dest,HotelLoc", resultsB, Option(TESTsize))


/////////////////////////////////////////////////////////////////////////////// 
///     C0:  MAKE PREDICTION BASED ON MOST FREQUENT CLUSTER AT DESTINATION, COUNTRY, MARKET, ISPACKAGE
///////////////////////////////////////////////////////////////////////////////  

        val resultsC0Map = TEST
        .map(q => ( (q.srch_destination_id, q.hotel.country, q.hotel.market, q.is_package), q)  )
        .cogroup(trainDataRowRDD.map{q => ((q.srch_destination_id, q.hotel.country,q.hotel.market,q.is_package), q)} )    //That's a huge shuffle !!WARNING to reduce the time repartition to 1k
        .filter{ case(key,value) => ((value._1.size>0) && (value._2.size>0)) }
        .flatMap{ case(key, value) => 
           var test = value._1 //matching querry data
           val train = value._2 //matching hotel location mapping best clusters


           val hotelClustersOrdered = train.map{ q => (q.hotel_cluster, append_1(q)) }.groupBy(_._1).map{ case(key,listKeyValuePairs) => (key, listKeyValuePairs.map(_._2).reduce(_+_))   }  //get Map of cluster -> counts
                                   .toSeq.sortWith{ case(a,b) =>  if( a._2 != b._2  ){a._2 > b._2}else{ clusterFrequencyBD.value(a._1) > clusterFrequencyBD.value(b._1) }  }                //order the list

           val hotelClustersMap     = hotelClustersOrdered.map((a=>a._1)).take(7).zipWithIndex.toMap  //only keep the 2 most likely clusters

            if(hotelClustersOrdered.size>1 && train.count(q => true)>10){  //changed from 50
              val attributes = getFeatureAttributes() 
              val labels:Array[scala.Int] = train.filter(q=> hotelClustersMap.contains(q.hotel_cluster)).map(q=> hotelClustersMap(q.hotel_cluster).toInt ).toArray
              val features:Array[Array[scala.Double] ] = train.filter(q=> hotelClustersMap.contains(q.hotel_cluster)).map(q => getFeature(q) ).toArray
              val classifier = new AdaBoost(attributes,  features.toArray, labels.toArray, 250, 3)

              test.map{q =>
                     val feature   = getFeature(q)
                     var posterior = Array.fill[scala.Double]( hotelClustersMap.size)(0.0)
                     classifier.predict(feature , posterior)
                   val prediction = posterior.toSeq.zipWithIndex.sortWith( (a,b) => a._1 * hotelClustersOrdered(a._2)._2 > b._1 * hotelClustersOrdered(b._2)._2).map(pair => hotelClustersOrdered(pair._2))
                     (q.id, result(q.hotel_cluster, (prediction ++ hotelClustersOrdered).distinct.take(10)), null)
              }
           }else{
              test.map{q => (q.id, result(q.hotel_cluster, hotelClustersOrdered.take(10)), null  ) }
           }
        }
        

        val resultsC0 = resultsC0Map.map(a => (a._1, a._2) ).cache()
//        Map5(EvaluatePerf,  "Destination Id, HotelLoc", resultsC, Option(TESTsize))



/////////////////////////////////////////////////////////////////////////////// 
///     C:  MAKE PREDICTION BASED ON MOST FREQUENT CLUSTER AT DESTINATION, COUNTRY, MARKET
///////////////////////////////////////////////////////////////////////////////  

        val resultsCMap = TEST
        .map(q => ( (q.srch_destination_id, q.hotel.country, q.hotel.market), q)  )
        .cogroup(trainDataRowRDD.map{q => ((q.srch_destination_id, q.hotel.country,q.hotel.market), q)} )    //That's a huge shuffle !!WARNING to reduce the time repartition to 1k
        .filter{ case(key,value) => ((value._1.size>0) && (value._2.size>0)) }
        .flatMap{ case(key, value) => 
           var test = value._1 //matching querry data
           val train = value._2 //matching hotel location mapping best clusters


           val hotelClustersOrdered = train.map{ q => (q.hotel_cluster, append_1(q)) }.groupBy(_._1).map{ case(key,listKeyValuePairs) => (key, listKeyValuePairs.map(_._2).reduce(_+_))   }  //get Map of cluster -> counts
                                   .toSeq.sortWith{ case(a,b) =>  if( a._2 != b._2  ){a._2 > b._2}else{ clusterFrequencyBD.value(a._1) > clusterFrequencyBD.value(b._1) }  }                //order the list

           val hotelClustersMap     = hotelClustersOrdered.map((a=>a._1)).take(7).zipWithIndex.toMap  //only keep the 2 most likely clusters

            if(hotelClustersOrdered.size>1 && train.count(q => true)>10){  //changed from 50
              val attributes = getFeatureAttributes() 
              val labels:Array[scala.Int] = train.filter(q=> hotelClustersMap.contains(q.hotel_cluster)).map(q=> hotelClustersMap(q.hotel_cluster).toInt ).toArray
              val features:Array[Array[scala.Double] ] = train.filter(q=> hotelClustersMap.contains(q.hotel_cluster)).map(q => getFeature(q) ).toArray
              val classifier = new AdaBoost(attributes,  features.toArray, labels.toArray, 250, 3)

              test.map{q =>
                     val feature   = getFeature(q)
                     var posterior = Array.fill[scala.Double]( hotelClustersMap.size)(0.0)
                     classifier.predict(feature , posterior)
                   val prediction = posterior.toSeq.zipWithIndex.sortWith( (a,b) => a._1 * hotelClustersOrdered(a._2)._2 > b._1 * hotelClustersOrdered(b._2)._2).map(pair => hotelClustersOrdered(pair._2))
                     (q.id, result(q.hotel_cluster, (prediction ++ hotelClustersOrdered).distinct.take(10)), null)
              }
           }else{
              test.map{q => (q.id, result(q.hotel_cluster, hotelClustersOrdered.take(10)), null  ) }
           }
        }
        

        val resultsC = resultsCMap.map(a => (a._1, a._2) ).cache()
//        Map5(EvaluatePerf,  "Destination Id, HotelLoc", resultsC, Option(TESTsize))

/////////////////////////////////////////////////////////////////////////////// 
///     D:  MAKE PREDICTION BASED ON MOST FREQUENT CLUSTER AT DESTINATION WITH MVA
///////////////////////////////////////////////////////////////////////////////  

        val resultsDMap = TEST
        .map(q => ( q.srch_destination_id, q)  )
        .cogroup(trainDataRowRDD.map{q => (q.srch_destination_id, q)} )    //That's a huge shuffle !!WARNING to reduce the time repartition to 1k
        .filter{ case(key,value) => ((value._1.size>0) && (value._2.size>0)) }
        .flatMap{ case(key, value) => 
           var test = value._1 //matching querry data
           val train = value._2 //matching hotel location mapping best clusters

           val hotelClustersOrdered = train.map{ q => (q.hotel_cluster, append_1(q)) }.groupBy(_._1).map{ case(key,listKeyValuePairs) => (key, listKeyValuePairs.map(_._2).reduce(_+_))   }  //get Map of cluster -> counts
                                   .toSeq.sortWith{ case(a,b) =>  if( a._2 != b._2  ){a._2 > b._2}else{ clusterFrequencyBD.value(a._1) > clusterFrequencyBD.value(b._1) }  }                //order the list


           val hotelClustersMap     = hotelClustersOrdered.map((a=>a._1)).take(5).zipWithIndex.toMap  //only keep the 2 most likely clusters

            if(hotelClustersOrdered.size>1 && train.count(q => true)>10){  //changed from 50
              val attributes = getFeatureAttributes() 
              val labels:Array[scala.Int] = train.filter(q=> hotelClustersMap.contains(q.hotel_cluster)).map(q=> hotelClustersMap(q.hotel_cluster).toInt ).toArray
              val features:Array[Array[scala.Double] ] = train.filter(q=> hotelClustersMap.contains(q.hotel_cluster)).map(q => getFeature(q) ).toArray
              val classifier = new AdaBoost(attributes,  features.toArray, labels.toArray, 500, 3)

              test.map{q =>
                     val feature   = getFeature(q)
                     var posterior = Array.fill[scala.Double]( hotelClustersMap.size)(0.0)
                     classifier.predict(feature , posterior)
                   val prediction = posterior.toSeq.zipWithIndex.sortWith( (a,b) => a._1 * hotelClustersOrdered(a._2)._2 > b._1 * hotelClustersOrdered(b._2)._2).map(pair => hotelClustersOrdered(pair._2))
                     (q.id, result(q.hotel_cluster, (prediction ++ hotelClustersOrdered).distinct.take(10)), null)
              }
           }else{
              test.map{q => (q.id, result(q.hotel_cluster, hotelClustersOrdered.take(10)), null  ) }
           }
        }
        val resultsD = resultsDMap.map(a => (a._1, a._2) ).cache()
//        Map5(EvaluatePerf,  "Destination Id", resultsD, Option(TESTsize))


/////////////////////////////////////////////////////////////////////////////// 
///     E0:  MAKE PREDICTION BASED ON MOST FREQUENT CLUSTER AT DESTINATION COUNTRY FOR SAME DESTINATION TYPE
///////////////////////////////////////////////////////////////////////////////  

        val hotelCountryDestTypeRDD = trainDataRowRDD                                                                  //WILL BE  PairRDD[Country , Top5clusters  ]
        .map(q => ( (q.hotel.country, q.srch_destination_type_id, q.hotel_cluster) , append_2(q))  )                                       //group by country/cluster
        .reduceByKey(_+_)                                                                                      //count per group
        .map( pair => {val key = pair._1; val value=pair._2; ((key._1, key._2), Seq( (key._3, value) ) ) })              //regroup by country/market
        .reduceByKey((a,b) => a++b )                                                                           //sum-up the list for each group
        .mapValues( a => a.sortWith{  case(a,b) =>  if( a._2 != b._2  ){a._2 > b._2}else{ clusterFrequencyBD.value(a._1) > clusterFrequencyBD.value(b._1) } }
                   )//       .map(_._1)  )                                                  //collapste various clusters to a list
 
        //c2) make the prediction
        val resultsE0 = TEST
        .map(q => ( (q.hotel.country, q.srch_destination_type_id), q)  )
        .cogroup(hotelCountryDestTypeRDD)
        .filter{ case(key,value) => ((value._1.size>0) && (value._2.size>0)) }
        .flatMap{ case(key, value) => 
           var test = value._1 //matching querry data
           val lookup = value._2 //matching hotel location mapping best clusters

           test.map{q =>  
                 (q.id, result(q.hotel_cluster, lookup.head.take(10) ))
          }
        }.cache()
//        Map5(EvaluatePerf,  "Hotel Country", resultsE0, Option(TESTsize))


/////////////////////////////////////////////////////////////////////////////// 
///     E:  MAKE PREDICTION BASED ON MOST FREQUENT CLUSTER AT DESTINATION COUNTRY 
///////////////////////////////////////////////////////////////////////////////  

        val hotelCountryRDD = trainDataRowRDD                                                                  //WILL BE  PairRDD[Country , Top5clusters  ]
        .map(q => ( (q.hotel.market, q.hotel_cluster) , append_2(q))  )                                       //group by country/cluster
        .reduceByKey(_+_)                                                                                      //count per group
        .map( pair => {val key = pair._1; val value=pair._2; (key._1, Seq( (key._2, value) ) ) })              //regroup by country/market
        .reduceByKey((a,b) => a++b )                                                                           //sum-up the list for each group
        .mapValues( a => a.sortWith{  case(a,b) =>  if( a._2 != b._2  ){a._2 > b._2}else{ clusterFrequencyBD.value(a._1) > clusterFrequencyBD.value(b._1) } }
                   )//       .map(_._1)  )                                                  //collapste various clusters to a list
 
        //c2) make the prediction
        val resultsE = TEST
        .map(q => ( q.hotel.market, q)  )
        .cogroup(hotelCountryRDD)
        .filter{ case(key,value) => ((value._1.size>0) && (value._2.size>0)) }
        .flatMap{ case(key, value) => 
           var test = value._1 //matching querry data
           val lookup = value._2 //matching hotel location mapping best clusters

           test.map{q =>  
                 (q.id, result(q.hotel_cluster, lookup.head.take(10) ))
          }
        }.cache()
        //Map5(EvaluatePerf,  "Hotel Country", resultsE, Option(TESTsize))

/////////////////////////////////////////////////////////////////////////////// 
///     F:  MAKE PREDICTION BASED ON MOST FREQUENT CLUSTER WORDLWIDE 
///////////////////////////////////////////////////////////////////////////////  

        val Top10HotelsWorldwide = clusterFrequency.take(10).toSeq//     .map(_._1).toSeq
        val resultsF = TEST
        .map(q => (q.id, result(q.hotel_cluster, Top10HotelsWorldwide)) ).cache()
        //Map5(EvaluatePerf,  "Cluster Frequencies", resultsF, Option(TESTsize))

   val ListWithId = TEST.map{ q => (q.id, result(q.hotel_cluster, Seq[(Int,Double)]() )) }.cache()


   Array[Future[Unit]](
      Future{ groupAndSave(EvaluatePerf, true, "groupAll", Array(ListWithId, resultsA, resultsB, resultsC0, resultsC, resultsD, resultsE0, resultsE, resultsF ) ) }

//      Future{ groupAndSave(EvaluatePerf, true, "AB", Array(ListWithId, resultsA, resultsB ) ) },
//      Future{ groupAndSave(EvaluatePerf, true, "aB", Array(ListWithId, resultsTestA, resultsB ) ) },
//      Future{ groupAndSave(EvaluatePerf, true, "A", Array(ListWithId, resultsA ) ) },
//      Future{ groupAndSave(EvaluatePerf, true, "a", Array(ListWithId, resultsTestA ) ) }

   ).foreach{f => Await.ready(f, 10000 minutes) }

    val totalelapsedTime = (System.nanoTime() - totalstartTime) / 1e9
    System.err.println(s"TOTAL time: $totalelapsedTime seconds")
    sc.stop()
    System.err.println("sparkContext stopped")
    System.exit(0)
  }
}


