#hadoop fs -rm  output/*
#hadoop fs -rmdir  output

#OUTPUTDIR=/home/mapr/DEV360/lab/output/
#rm -rd $OUTPUTDIR
#rm -rdf /afs/cern.ch/user/q/querten/workspace/public/SparkTest/Expedia/ExpediaRandomForest
../spark-1.6.1-bin-hadoop2.6/bin/spark-submit --class lq.ExpediaSQL --master local[*] target/Expedia-1.0.jar #/user/mapr/input/alice.txt $OUTPUTDIR
