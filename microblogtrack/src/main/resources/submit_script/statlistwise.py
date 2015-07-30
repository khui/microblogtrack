from time import gmtime
import getopt, sys, os
from operator import itemgetter

opts,args=getopt.getopt(sys.argv[1:],'e:d:',["expname=","directory="])
delimiter=' '
for opt,arg in opts:
    if opt in ('-e','--expname'):
        expname=str(arg)
    elif opt in ('-d','--directory'):
        subdir=str(arg)


def statlw(expf, expn):
    lines=dict()
    for line in open(expf):
        cols = line.split(' ')
        qidstr=str(cols[1])
        tweetid=long(cols[3])
        runid=str(cols[6]).rstrip('\n')
        date=int(cols[0])
        intqid=int(qidstr.lstrip('MB'))
        if intqid not in lines:
            lines[intqid]=dict()
        if date not in lines[intqid]:
            lines[intqid][date]=dict()
        if tweetid not in lines[intqid][date]:
            lines[intqid][date][tweetid]=list()
        lines[intqid][date][tweetid].append((qidstr, tweetid, runid, date, intqid))
    for qid in sorted(lines.keys()):
        datecount=list()
        for date in sorted(lines[qid].keys()):
            datecount.append(str(date) + ":" + str(len(lines[qid][date])))
        print expn, qid, len(datecount), ' '.join(datecount)
        
subdir="/local/home/khui/workspace/javaworkspace/twitter-localdebug/onlinetask/B"

for f in os.listdir(subdir):
    if not f.endswith('trec'):
        continue
    expn = f.split('.')[0]
    statlw(os.path.join(subdir, f), expn)
