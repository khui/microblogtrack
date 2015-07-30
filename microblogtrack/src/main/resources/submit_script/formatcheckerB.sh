dir=/local/home/khui/workspace/javaworkspace/twitter-localdebug/onlinetask/B
for f in `ls $dir`
do
	filename=$(echo $f | awk -F. '{print $1}')
	suffix=$(echo $f | awk -F. '{print $2}')
	if [ $suffix != "trec" ]
	then
		continue
	fi
	echo $f is being checked
	./check-microblog.pl B $dir/$f
done
