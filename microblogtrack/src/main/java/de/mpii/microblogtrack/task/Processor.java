package de.mpii.microblogtrack.task;

import de.mpii.microblogtrack.component.DecisionMakerTimer;
import de.mpii.microblogtrack.component.core.ListwiseDecisionMaker;
import de.mpii.microblogtrack.component.core.LuceneDMConnector;
import de.mpii.microblogtrack.component.core.lucene.LuceneScorer;
import de.mpii.microblogtrack.component.core.PointwiseDecisionMaker;
import de.mpii.microblogtrack.component.predictor.PointwiseScorer;
import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.LoadProperties;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import de.mpii.microblogtrack.utility.io.printresult.ResultPrinter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.classic.ParseException;
import twitter4j.TwitterException;

/**
 * entrance for the whole project, there are two subclass implements different
 * user case: online and offline. online is dealing with the live api stream,
 * whereas offline processor is to process stream from a local file
 *
 * @author khui
 */
public abstract class Processor {

    static Logger logger = Logger.getLogger(Processor.class.getName());

    protected final BlockingQueue<String> api2indexqueue = new LinkedBlockingQueue<>(2000);

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
     * @param expandqueryfile
     * @param outdir
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     * @throws java.util.concurrent.ExecutionException
     * @throws org.apache.lucene.queryparser.classic.ParseException
     * @throws java.lang.ClassNotFoundException
     * @throws java.lang.InstantiationException
     * @throws java.lang.IllegalAccessException
     * @throws twitter4j.TwitterException
     * @throws java.lang.NoSuchMethodException
     */
    public void start(String datadir, String indexdir, String queryfile, String expandqueryfile, String outdir) throws IOException, InterruptedException, ExecutionException, ParseException, ClassNotFoundException, InstantiationException, IllegalAccessException, TwitterException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException {
        // communication between lucene search results and pointwise decision maker
        BlockingQueue<QueryTweetPair> queueLucene2PointwiseDM = new LinkedBlockingQueue<>(2000);
        BlockingQueue<QueryTweetPair> queueLucene2ListwiseDM = new LinkedBlockingQueue<>(2000);
        Map<String, LuceneDMConnector> queryTrackers = new HashMap<>(250);
        PointwiseScorer pwpredictor = (PointwiseScorer) Class.forName(Configuration.POINTWISE_PREDICTOR).newInstance();
        LuceneScorer lscorer = new LuceneScorer(indexdir, queryTrackers, pwpredictor);
        receiveStatus(lscorer, datadir, Configuration.LISTENER_THREADNUM);
        lscorer.multiQuerySearch(queryfile, expandqueryfile, queueLucene2PointwiseDM, queueLucene2ListwiseDM);
        // set up output writer to print out the notification task results
        ResultPrinter resultprinterpw = new ResultPrinter(outdir + "/pointwise");
        ResultPrinter resultprinterlw = new ResultPrinter(outdir + "/listwise");
        Constructor<?> decisionmaker = Class.forName(Configuration.LW_DM_METHOD).getConstructor(Map.class, BlockingQueue.class, ResultPrinter.class);
        DecisionMakerTimer periodicalStartPointwiseDM = new DecisionMakerTimer(new PointwiseDecisionMaker(queryTrackers, queueLucene2PointwiseDM, resultprinterpw), "PW-DM", 5);
        DecisionMakerTimer periodicalStartListwiseDM = new DecisionMakerTimer((ListwiseDecisionMaker) decisionmaker.newInstance(queryTrackers, queueLucene2ListwiseDM, resultprinterlw), "LW-DM", 5);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

        scheduler.scheduleAtFixedRate(periodicalStartPointwiseDM, Configuration.DM_START_DELAY, Configuration.PW_DM_PERIOD, Configuration.TIMEUNIT);
        scheduler.scheduleAtFixedRate(periodicalStartListwiseDM, Configuration.DM_START_DELAY, Configuration.LW_DM_PERIOD, Configuration.TIMEUNIT);
        /**
         * check whether the hbc output queue get stuck
         */
        while (!Thread.interrupted()) {
            if (api2indexqueue.size() >= 2000) {
                logger.error("api2indexqueue is full: " + api2indexqueue.size());
                try {
                    Thread.sleep(1000 * 60 * 10);
                } catch (InterruptedException ex) {
                }
            }
        }
    }

    protected abstract void receiveStatus(LuceneScorer lscorer, String dataORkeydir, int numProcessingThreads);

    public static void main(String[] args) throws InterruptedException, ExecutionException, ParseException, ClassNotFoundException, InstantiationException, IllegalAccessException, IOException, TwitterException, org.apache.commons.cli.ParseException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException {

        Options options = new Options();
        options.addOption("o", "outfile", true, "output file");
        options.addOption("d", "dataORkeydirectory", true, "data/api key directory");
        options.addOption("i", "indexdirectory", true, "index directory");
        options.addOption("q", "queryfile", true, "query file");
        options.addOption("e", "expandqueryfile", true, "expanded query file");
        options.addOption("s", "meanstdscalefile", true, "scale parameters for feature normalization");
        options.addOption("l", "log4jxml", true, "log4j conf file");
        options.addOption("p", "property", true, "property file");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        String outputdir = null, data_key_dir = null, indexdir = null, queryfile = null, expandqueryfile = null, scalefile = null, propertyfile = null,
                log4jconf = null;
        if (cmd.hasOption("o")) {
            outputdir = cmd.getOptionValue("o");
        }
        if (cmd.hasOption("l")) {
            log4jconf = cmd.getOptionValue("l");
        }
        if (cmd.hasOption("d")) {
            data_key_dir = cmd.getOptionValue("d");
        }
        if (cmd.hasOption("i")) {
            indexdir = cmd.getOptionValue("i");
        }
        if (cmd.hasOption("q")) {
            queryfile = cmd.getOptionValue("q");
        }
        if (cmd.hasOption("e")) {
            expandqueryfile = cmd.getOptionValue("e");
        }
        if (cmd.hasOption("s")) {
            scalefile = cmd.getOptionValue("s");
        }
        if (cmd.hasOption("p")) {
            propertyfile = cmd.getOptionValue("p");
        }

        /**
         * for local test
         */
        String rootdir = "/home/khui/workspace/javaworkspace/twitter-localdebug";
        indexdir = rootdir + "/index";
        queryfile = rootdir + "/queries/TREC2015-MB-testtopics.txt";
        //rootdir + "/queries/fusion.title";
        expandqueryfile = rootdir + "/queries/queryexpansion15.res";
        //rootdir + "/queries/queryexpansion.res";
        data_key_dir = rootdir + "/tweetzipklein";
        //data_key_dir = rootdir + "/twitterkeys";
        scalefile = rootdir + "/scale_file/scaler.meanstd";
        outputdir = rootdir + "/outputdir";
        log4jconf = "src/main/java/log4j.xml";
        //propertyfile = rootdir + "/online-debug-property.config";
        propertyfile = rootdir + "/local-debug-property.config";
        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);
        LoadProperties.load(propertyfile);
        logger.info("online proecss start.");
        //Processor op = new OnlineProcessor();
        Processor op = new OfflineProcessor();
        op.start(data_key_dir, indexdir, queryfile, expandqueryfile, outputdir);

    }
}
