package de.mpii.microblogtrack.task;

import de.mpii.microblogtrack.component.LuceneScorer;
import de.mpii.microblogtrack.userprofiles.TrecQuery;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import de.mpii.microblogtrack.utility.io.ReadStatus;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.lucene.queryparser.classic.ParseException;
import twitter4j.Status;
import twitter4j.TwitterException;

/**
 *
 * @author khui
 */
public class OfflineTrainer {

    public void process(String indexdir, String queryfile, BlockingQueue<Status> intbq, BlockingQueue<QueryTweetPair> outbq) throws IOException, InterruptedException, ParseException, ExecutionException {
        LuceneScorer lscorer = new LuceneScorer(indexdir);
        TrecQuery tq = new TrecQuery();
        //Map<String, Query> queries = tq.readInQueries(queryfile);
        //lscorer.multiQuerySearch(outbq, queries);
        Status status;
        long tweetcountId = 0;
        int indexcount = 0;
        while (indexcount < 8000) {
            status = intbq.poll(100, TimeUnit.MILLISECONDS);
            if (status != null) {
                tweetcountId = lscorer.write2Index(status);
                if (tweetcountId > 0) {
                    indexcount++;
                }
                if (tweetcountId % 400 == 0) {
                    lscorer.multiScorerDemo();
                }

            } else {
                System.err.println("status is null: " + outbq.size() + " " + intbq.size());
            }
        }

        //lscorer.multiScorerDemo(tweetcountId);
        lscorer.closeWriter();
    }

//    public static void main(String[] args) throws InterruptedException, IOException, ParseException, ExecutionException, TwitterException {
//        org.apache.log4j.PropertyConfigurator.configure("src/main/java/log4j.xml");
//        LogManager.getRootLogger().setLevel(Level.INFO);
//        String dir = "/home/khui/workspace/javaworkspace/twitter-localdebug";
//        String queryfile = "/home/khui/workspace/result/data/query/microblog/11";
//        String gzipDir = "/home/khui/workspace/javaworkspace/twitter-localdebug/gzipdump";
//        String indexdir = dir + "/index";
//        System.out.println("start to process");
//        //LangFilterLD.loadprofile(dir + "/lang-dect-profile");
//        BlockingQueue<Status> inbq = new LinkedBlockingQueue<>(100);
//        BlockingQueue<QueryTweetPair> outbq = new LinkedBlockingQueue<>(10000);
//        ExecutorService service = Executors.newSingleThreadExecutor();
//        service.submit(new ReadStatus(gzipDir, inbq));
//        OfflineTrainer ot = new OfflineTrainer();
//        ot.process(indexdir, queryfile, inbq, outbq);
//        service.shutdownNow();
//    }
}
