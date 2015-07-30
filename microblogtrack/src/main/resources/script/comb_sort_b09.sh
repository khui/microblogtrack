timestamp=`date +"%m%d%M%H"`
expname=comb_sort_$timestamp_`hostname`
echo $expname

javaworkspace=/home/khui/workspace/javaworkspace
jdk=$javaworkspace/java-8-sun/jdk1.8.0_45
workdir=/scratch/GW/pool0/khui/result/microblogtrack/onlinetask
indexdir=$workdir/index/$expname
outdir=$workdir/output/$expname
queryfile=/GW/D5data-2/khui/microblogtrack/queries/TREC2015-MB-testtopics.txt
equeryfile=/GW/D5data-2/khui/microblogtrack/queries/queryexpansion15.res
keydir=/GW/D5data-2/khui/microblogtrack/apikeys/batchkeys/taskprocess
configfile=/home/khui/workspace/script/microblogtrack/properties/comb_sort.config
log4jconf=/home/khui/workspace/javaworkspace/log4j.xml


if [ -d $outdir ]
then
	echo $outdir exists
	exit
#	rm -rf $outdir
fi
if [ -d $indexdir ]
then
	echo $indexdir exists
#	rm -rf $indexdir
fi
mkdir -p $indexdir $outdir


export HADOOP_CLASSPATH=`find /home/khui/workspace/javaworkspace/lib/microblogmaven -name "*.jar" | tr '\n' ':'`
export HADOOP_OPTS="-Xms12G -Xmx12G  -XX:+UseConcMarkSweepGC"
export JAVA_HOME=${jdk}
export PATH=$PATH:$JAVA_HOME/bin
	


START=$(date +%s.%N)
hadoop jar $javaworkspace/microblogtrack.jar \
	   de.mpii.microblogtrack.task.Processor \
	   -d $keydir \
	   -i $indexdir \
	   -q $queryfile \
	   -l $log4jconf \
	   -o $outdir \
	   -p $configfile \
	   -e $equeryfile
END=$(date +%s.%N)
DIFF=$(echo "$END - $START" | bc)
echo $expid $apikey finished within $DIFF

