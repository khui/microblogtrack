dirs="/local/home/khui/workspace/javaworkspace/twitter-localdebug/onlinetask/output"
outdir="/local/home/khui/workspace/javaworkspace/twitter-localdebug/onlinetask/A"
if [ -d $outdir ]
then
	echo removing $outdir
	rm -rf $outdir
fi
mkdir $outdir
for dir in `ls $dirs`
do
	cat $dirs/$dir/pointwise/*.trec | awk '$3<=1438214399 {print}' > $outdir/$dir.trec
	cat $dirs/$dir/pointwise/*.log > $outdir/$dir.log
	cat $outdir/$dir.log | awk -F$'\t' 'NF==5 {print $NF}' | awk '{print $3}' |sort -k1,1 | uniq -c > $outdir/$dir.stat
	echo $dir `cat $outdir/$dir.trec | wc -l` 
done
