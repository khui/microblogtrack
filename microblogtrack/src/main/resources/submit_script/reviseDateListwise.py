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


def println(expf, outfstr):
    date=20150719
    qidDate=dict()
    outf=open(outfstr, 'w')
    for line in open(expf):
        cols = line.split(' ')
        qidstr=str(cols[1])
        tweetid=str(cols[3])
        rank=int(cols[4])
        score=str(cols[5])
        runid=str(cols[6]).rstrip('\n')
        if runid=="MPII_COMB_MAXREP":
            runid="MPII_COM_MAXREP"
        if rank==1:
            if qidstr not in qidDate:
                qidDate[qidstr]=date
            qidDate[qidstr]+=1
        linestr=(str(qidDate[qidstr]), qidstr, "Q0", tweetid, str(rank), score, runid, '\n')
        outf.write(' '.join(linestr))
    outf.close()
        
subdir="/local/home/khui/workspace/javaworkspace/twitter-localdebug/onlinetask/B/raw"
outdir="/local/home/khui/workspace/javaworkspace/twitter-localdebug/onlinetask/B"

for f in os.listdir(subdir):
    if not f.endswith('trec'):
        continue
    expn = f.split('.')[0]
    println(os.path.join(subdir, f), os.path.join(outdir, f))
    print "finished", f
