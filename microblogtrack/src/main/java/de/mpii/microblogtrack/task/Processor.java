package de.mpii.microblogtrack.task;

import de.mpii.microblogtrack.component.DecisionMakerTimer;
import de.mpii.microblogtrack.component.ListwiseDecisionMaker;
import de.mpii.microblogtrack.component.LuceneScorer;
import de.mpii.microblogtrack.component.PointwiseDecisionMaker;
import de.mpii.microblogtrack.component.ResultTweetsTracker;
import de.mpii.microblogtrack.component.predictor.PointwiseScorer;
import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.LibsvmWrapper;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import de.mpii.microblogtrack.utility.io.printresult.ResultPrinter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.queryparser.classic.ParseException;
import twitter4j.TwitterException;

/**
 *
 * @author khui
 */
public abstract class Processor {

    /**
     * for notification task: 1) search the results and retain top-k per minute
     * per query 2) the tracker keep track all the search results for each
     * query: the centroids, as reference for distance between tweets, and the
     * score distribution to generate relative score 3) the pointwise prediction
     * scores are computed by pointwise scorer 4) the search results are passed
     * to the decision maker through blocking queue 5) the decision maker will
     * re-called each day
     *
     * for email digest task: 1) ... 2)... 3) the listwise decision maker will
     * keep recording all tweets from lucene which is beyond the
     * LW_DM_SCORE_FILTER, and keeping top-LW_DM_QUEUE_LEN tweets for each
     * query, sorted by absolute point wise prediction score 4) at the end of
     * each day, we will run max-rep algorithm to select top-LW_DM_SELECTNUM
     * tweets and print with resultprinter
     *
     * @param datadir
     * @param indexdir
     * @param queryfile
     * @param outdir
     * @param scalefile
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     * @throws java.util.concurrent.ExecutionException
     * @throws org.apache.lucene.queryparser.classic.ParseException
     * @throws java.lang.ClassNotFoundException
     * @throws java.lang.InstantiationException
     * @throws java.lang.IllegalAccessException
     * @throws twitter4j.TwitterException
     */
    public void start(String datadir, String indexdir, String queryfile, String outdir, String scalefile) throws IOException, InterruptedException, ExecutionException, ParseException, ClassNotFoundException, InstantiationException, IllegalAccessException, TwitterException {
        // communication between lucene search results and pointwise decision maker
        BlockingQueue<QueryTweetPair> queueLucene2PointwiseDM = new LinkedBlockingQueue<>(5000);
        BlockingQueue<QueryTweetPair> queueLucene2ListwiseDM = new LinkedBlockingQueue<>(5000);
        Map<String, ResultTweetsTracker> queryTrackers = new HashMap<>(250);
        LuceneScorer lscorer = new LuceneScorer(indexdir, queryTrackers, new PointwiseScorer(), LibsvmWrapper.readScaler(scalefile));
        receiveStatus(lscorer, datadir, 1);
        lscorer.multiQuerySearch(queryfile, queueLucene2PointwiseDM, queueLucene2ListwiseDM);
        // set up output writer to print out the notification task results
        ResultPrinter resultprinterpw = new ResultPrinter(outdir + "/pointwise");
        ResultPrinter resultprinterlw = new ResultPrinter(outdir + "/listwise");
        DecisionMakerTimer decisionMakerTimerPW = new DecisionMakerTimer(new PointwiseDecisionMaker(queryTrackers, queueLucene2PointwiseDM, resultprinterpw), 1);
        DecisionMakerTimer decisionMakerTimerLW = new DecisionMakerTimer(new ListwiseDecisionMaker(queryTrackers, queueLucene2ListwiseDM, resultprinterlw), 2);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        scheduler.scheduleAtFixedRate(decisionMakerTimerPW, Configuration.DM_START_DELAY, Configuration.PW_DM_PERIOD, TimeUnit.MINUTES);
        scheduler.scheduleAtFixedRate(decisionMakerTimerLW, Configuration.DM_START_DELAY, Configuration.LW_DM_PERIOD, TimeUnit.MINUTES);
    }

    protected abstract void receiveStatus(LuceneScorer lscorer, String dataORkeydir, int numProcessingThreads);

}
