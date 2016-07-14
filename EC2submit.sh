#hadoop fs -rm  output/*
#hadoop fs -rmdir  output

#OUTPUTDIR=/home/mapr/DEV360/lab/output/
#rm -rd $OUTPUTDIR


export AWS_ACCESS_KEY_ID="AKIAIOS2SAB475EU3TYQ"
export AWS_SECRET_ACCESS_KEY="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"
#spark-ec2 --key-pair=Loicus --identity-file=/home/mapr/.ssh/Loicus.pem  --region=us-west-2 --zone=us-west-2a --instance-type=t2.micro    -s 2 launch loicus-spark-cluster
spark-ec2 -k Loicus -i /home/mapr/.ssh/Loicus.pem --region=us-west-2 --zone=us-west-2a login loicus-spark-cluster

#spark-ec2 destroy loicus-spark-cluster

#spark-submit --class lq.Expedia --master local[*] target/Expedia-1.0.jar #/user/mapr/input/alice.txt $OUTPUTDIR


#spark-submit --class lq.Expedia --master spark://ec2-52-39-80-3.us-west-2.compute.amazonaws.com:7077 --executor-memory 1G --total-executor-cores 2  target/Expedia-1.0.jar
