dir=/local/home/khui/workspace/javaworkspace/twitter-localdebug/onlinetask/A
errdir=/local/home/khui/workspace/javaworkspace/twitter-localdebug/onlinetask/errlog/A
for f in `ls $dir`
do
	filename=$(echo $f | awk -F. '{print $1}')
	suffix=$(echo $f | awk -F. '{print $2}')
	if [ $suffix != "trec" ]
	then
		continue
	fi
	if [ $filename == "luc_sort_d5blade21" -o $filename == "comb_sort_d5blade09" -o $filename == "hybrid_pw" ]
	then
		echo $f is being checked
		./check-microblog.pl A $dir/$f
	fi
done
