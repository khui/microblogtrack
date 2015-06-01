id=`date +"%m%d%M%H"`.`hostname`.$BASHPID
echo $id
expname=samplehbc

javaworkspace=/home/khui/workspace/javaworkspace
jdk=$javaworkspace/java-8-sun/jdk1.8.0_45
outdir=/GW/D5data-2/khui/microblogtrack/crawledtweets/$expname
keydir=/GW/D5data-2/khui/microblogtrack/apikeys/batchkeys/apikey4

if [ -d $outdir ]
then
        echo removing $outdir
        sleep 10
        rm -rf $outdir
fi
mkdir -p $outdir


export HADOOP_CLASSPATH=`find /home/khui/workspace/javaworkspace/lib/microblog -name "*.jar" | tr '\n' ':'`
export HADOOP_OPTS="-Xms5G -Xmx5G"
export JAVA_HOME=${jdk}
export PATH=$PATH:$JAVA_HOME/bin



START=$(date +%s.%N)
hadoop jar $javaworkspace/crawltweet.jar \
           crawltweets.ListenerAndDumper \
           -o $outdir \
           -k $keydir
END=$(date +%s.%N)
DIFF=$(echo "$END - $START" | bc)
echo $expid $apikey finished within $DIFF