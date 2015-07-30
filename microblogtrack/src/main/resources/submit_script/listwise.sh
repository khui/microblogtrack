dirs="/local/home/khui/workspace/javaworkspace/twitter-localdebug/onlinetask/output"
outdir="/local/home/khui/workspace/javaworkspace/twitter-localdebug/onlinetask/B"
if [ -d $outdir ]
then
	echo removing $outdir
	rm -rf $outdir
fi
mkdir $outdir $outdir/raw
for dir in `ls $dirs`
do
	cat $dirs/$dir/listwise/*.trec > $outdir/raw/$dir.trec
	cat $dirs/$dir/listwise/*.log > $outdir/$dir.log
	cat $outdir/$dir.log | awk -F$'\t' 'NF==5 {print $NF}' | awk '{print $3}' |sort -k1,1 | uniq -c > $outdir/$dir.stat
	echo $dir `cat $outdir/raw/$dir.trec | wc -l` 
done