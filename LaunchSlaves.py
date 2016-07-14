#!/usr/bin/env python

import urllib
import string
import os
import sys
import commands

FarmDirectory = "FARM"
JobName = "Test"
Jobs_Queue = "8nh"
SparkDir = "/afs/cern.ch/user/q/querten/workspace/public/SparkTest/spark-1.6.1-bin-hadoop2.6/"
MasterHostName = commands.getstatusoutput('hostname')[1]
SlaveList = []

if sys.argv[1]=='start':
   print "Start Master"
   commands.getstatusoutput(SparkDir+"/sbin/start-master.sh")

if sys.argv[1]=='start' or sys.argv[1]=='startslaves':
   print "Start Slaves"
   for i in range (1,200) :      
      cmd = "bsub -q " + Jobs_Queue + " -J " + JobName+str(i) + " -R \" span[ptile=4]\" " + " 'JAVAVERSION=$( java -version 2>&1 >/dev/null ); test \"${JAVAVERSION#*'1.8.0'}\" != \"$JAVAVERSION\" && ( export SPARK_WORKER_MEMORY=8g; export SPARK_EXECUTOR_MEMORY=8g;export SPARK_WORKER_DIR=$(pwd);export SPARK_WORKER_OPTS=\" -Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.appDataTtl=8000\"; "+SparkDir+"/bin/spark-class org.apache.spark.deploy.worker.Worker spark://"+MasterHostName+":7077 --cores 4 --memory 4G )'"
      #print cmd
      out = commands.getstatusoutput(cmd)
      JobIndex = out[1].split('<')[1].split('>')[0]
      SlaveList += [JobIndex]

if sys.argv[1]=='submit':
   print "Run Job"
#   os.system("rm -rdf /afs/cern.ch/user/q/querten/workspace/public/SparkTest/Expedia/ExpediaRandomForest")
   print (SparkDir+"/bin/spark-submit --class lq.Expedia --master spark://"+MasterHostName+":7077 ../target/Expedia-1.0.jar")
   os.system(SparkDir+"/bin/spark-submit --executor-memory 4G --class lq.Expedia --master spark://"+MasterHostName+":7077 ../target/Expedia-1.0.jar ")


if sys.argv[1]=='submitAll':
   for i in range(1,10) :
      list = ''
      for j in range(i*10,i*10+10) :
         list += " " + str(j)
      print "Run Job for cluster hotel " + str(i)
      command = SparkDir+"/bin/spark-submit --executor-memory 4G --class lq.Expedia --master spark://"+MasterHostName+":7077 ../target/Expedia-1.0.jar /afs/cern.ch/user/q/querten/workspace/public/SparkTest/Expedia/ExpediaRandomForest/" + list + " 1> STDOUTL"+str(i) +" 2>/tmp/LQ_STDERR; tail -n 500 /tmp/LQ_STDERR > STDERRL" + str(i) + ";"   
      print (command)
      os.system(command)

if sys.argv[1]=='submitDF':
   print "Run Job"
   print (SparkDir+"/bin/spark-submit --class lq.ExpediaWithDF --master spark://"+MasterHostName+":7077 ../target/Expedia-1.0.jar")
   os.system(SparkDir+"/bin/spark-submit --executor-memory 4G --class lq.ExpediaWithDF --master spark://"+MasterHostName+":7077 ../target/Expedia-1.0.jar ")

if sys.argv[1]=='submitSQL':
   os.system("rm -rdf /afs/cern.ch/user/q/querten/scratch0/Expedia_16_05_14")
   os.system("rm -rdf /afs/cern.ch/user/q/querten/scratch0/Expedia/*")
   os.system(SparkDir+"/bin/spark-submit --executor-memory 4G --class lq.ExpediaSQL --master spark://"+MasterHostName+":7077 ../target/Expedia-1.0.jar ")
   os.system("echo \"id,hotel_cluster\" > validation.csv; cat /afs/cern.ch/user/q/querten/scratch0/Expedia/groupAll/part-* | sort -g | head -n 1000 >> validation.csv")


if sys.argv[1]=='stop':
   print "Stop Slaves"
   for slave in SlaveList:
      os.system("bkill " + slave)

   print "Stop Master"
   commands.getstatusoutput(SparkDir+"/sbin/stop-master.sh")

