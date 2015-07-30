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

exp0="luc"
exp1="comb"
listwise="sort"
runid="MPII_HYBRID_PW"
def extract(expf, expn, lines):
    for line in open(expf):
        cols = line.split(' ')
        qidstr=str(cols[0])
        tweetid=long(cols[1])
        epoch=int(cols[2])
        runid=str(cols[3]).rstrip('\n')
        date=int(gmtime(float(epoch)).tm_mday)
        intqid=int(qidstr.lstrip('MB'))
        if intqid not in lines:
            lines[intqid]=dict()
        if date not in lines[intqid]:
            lines[intqid][date]=dict()
        if tweetid not in lines[intqid][date]:
            lines[intqid][date][tweetid]=list()
        lines[intqid][date][tweetid].append((qidstr, tweetid, epoch, runid, date, intqid))

subdir="/local/home/khui/workspace/javaworkspace/twitter-localdebug/onlinetask/A"
outf=open(os.path.join(subdir,"hybrid_pw.trec"),'w')
def println(lines):
    epoch=0
    for line in lines:
        qidstr=line[0]
        tweetid=str(line[1])
        epochl=int(line[2])
        if epochl > epoch:
            epoch=epochl
    line=(qidstr, str(tweetid), str(epoch), runid,'\n')
    outf.write(' '.join(line))





lines=dict()
for f in os.listdir(subdir):
    if not f.endswith('trec'):
        continue
    fnamecols=f.split('_')
    if fnamecols[1] != listwise:
        continue
    if fnamecols[0] == exp0 or fnamecols[0] == exp1:
        expn = f.split('.')[0]
        extract(os.path.join(subdir, f), expn, lines)
for qid in sorted(lines.keys()):
    for date in sorted(lines[qid].keys()):
        first=True
        for tweetid in sorted(lines[qid][date], key=lambda k:len(lines[qid][date][k]), reverse=True):
            if first or len(lines[qid][date][tweetid])==2:
                #print qid, date, len(lines[qid][date][tweetid])
                first=False
                println(lines[qid][date][tweetid])
outf.close()
