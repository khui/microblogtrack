package de.mpii.microblogtrack.task;

import de.mpii.microblogtrack.component.DecisionMakerTimer;
import de.mpii.microblogtrack.component.LuceneScorer;
import de.mpii.microblogtrack.component.PointwiseDecisionMaker;
import de.mpii.microblogtrack.component.predictor.PointwiseScorer;
import de.mpii.microblogtrack.utility.LibsvmWrapper;
import de.mpii.microblogtrack.utility.MYConstants;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import de.mpii.microblogtrack.utility.ResultTweetsTracker;
import de.mpii.microblogtrack.utility.io.printresult.ResultPrinter;
import de.mpii.microblogtrack.utility.io.printresult.WriteTrecSubmission;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.classic.ParseException;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

/**
 * this class mainly for test sake: simulate the api real-time stream with
 * offline data, so that we may use past year labeled data to test the
 * performance of our notification/e-mail digest task
 *
 * @author khui
 */
public class OfflineProcessor {

    static Logger logger = Logger.getLogger(OfflineProcessor.class.getName());

    private class ReadInTweets implements Runnable {

        private final String datadir;

        private final LuceneScorer lscorer;

        public ReadInTweets(LuceneScorer lscorer, String datadir) {
            this.lscorer = lscorer;
            this.datadir = datadir;
        }

        /**
         * instead of listening to the api, we directly read the tweet from
         * files, here we mainly use twitter2011 dataset, which is in zip file,
         * within each of which there is multiple files, and each of them is a
         * tweet
         *
         * @param lscorer
         * @param datadir
         * @param numProcessingThreads
         */
        @Override
        public void run() {
            ZipFile zipf;
            String jsonstr;
            BufferedReader br;
            StringBuilder sb;
            File datasetDir = new File(datadir);
            int inputtweetcount = 0;
            for (File f : datasetDir.listFiles()) {
                if (f.getName().endsWith("zip")) {
                    try {
                        zipf = new ZipFile(f);
                        Enumeration<? extends ZipEntry> entries = zipf.entries();
                        while (entries.hasMoreElements()) {
                            ZipEntry ze = (ZipEntry) entries.nextElement();
                            br = new BufferedReader(
                                    new InputStreamReader(zipf.getInputStream(ze)));
                            sb = new StringBuilder();
                            while (br.ready()) {
                                sb.append(br.readLine());
                            }
                            jsonstr = sb.toString();
                            lscorer.write2Index(TwitterObjectFactory.createStatus(jsonstr));
                            br.close();
                            inputtweetcount++;
                            if (inputtweetcount % 6000 == 0) {
                                Thread.sleep(1000 * 60);
                            }
                        }
                        zipf.close();
                    } catch (IOException | TwitterException | InterruptedException ex) {
                        logger.error("readInTweets", ex);
                    }
                    logger.info("read in " + f.getName() + " finished");
                }
            }
            logger.info("finished read in all");

        }
    }

    /**
     * for notification task: 1) search the results and retain top-k per minute
     * per query 2) the tracker keep track all the search results for each
     * query: the centroids, as reference for distance between tweets, and the
     * score distribution to generate relative score 3) the pointwise prediction
     * scores are computed by pointwise scorer 4) the search results are passed
     * to the decision maker through blocking queue 5) the decision maker will
     * re-called each day
     *
     * @param datadir
     * @param indexdir
     * @param queryfile
     * @param outfile
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
    public void notificationTask(String datadir, String indexdir, String queryfile, String outfile, String scalefile) throws IOException, InterruptedException, ExecutionException, ParseException, ClassNotFoundException, InstantiationException, IllegalAccessException, TwitterException {
        // communication between lucene search results and pointwise decision maker
        BlockingQueue<QueryTweetPair> querytweetpairs = new LinkedBlockingQueue<>(10000);
        Map<String, ResultTweetsTracker> queryTrackers = new HashMap<>(250);
        LuceneScorer lscorer = new LuceneScorer(indexdir, queryTrackers, new PointwiseScorer(), LibsvmWrapper.readScaler(scalefile));
        Executor excutor = Executors.newSingleThreadExecutor();
        excutor.execute(new ReadInTweets(lscorer, datadir));
        lscorer.multiQuerySearch(queryfile, querytweetpairs);
        // set up output writer to print out the notification task results
        ResultPrinter resultprinter = new WriteTrecSubmission(outfile);
        DecisionMakerTimer decisionMakerTimer = new DecisionMakerTimer(new PointwiseDecisionMaker(queryTrackers, querytweetpairs, resultprinter));
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(decisionMakerTimer, MYConstants.DECISION_MAKER_START_DELAY, MYConstants.DECISION_MAKER_PERIOD, TimeUnit.MINUTES);
    }

    public static void main(String[] args) throws TwitterException, org.apache.commons.cli.ParseException, InterruptedException {
        Options options = new Options();
        options.addOption("o", "outfile", true, "output file");
        options.addOption("d", "datadirectory", true, "data directory");
        options.addOption("i", "indexdirectory", true, "index directory");
        options.addOption("q", "queryfile", true, "query file");
        options.addOption("s", "meanstdscalefile", true, "scale parameters for feature normalization");
        options.addOption("l", "log4jxml", true, "log4j conf file");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        String outputfile = null, datadir = null, indexdir = null, queryfile = null, scalefile = null, log4jconf = null;
        if (cmd.hasOption("o")) {
            outputfile = cmd.getOptionValue("o");
        }
        if (cmd.hasOption("l")) {
            log4jconf = cmd.getOptionValue("l");
        }
        if (cmd.hasOption("d")) {
            datadir = cmd.getOptionValue("d");
        }
        if (cmd.hasOption("i")) {
            indexdir = cmd.getOptionValue("i");
        }
        if (cmd.hasOption("q")) {
            queryfile = cmd.getOptionValue("q");
        }
        if (cmd.hasOption("s")) {
            scalefile = cmd.getOptionValue("s");
        }
        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);
        logger.info("offline process test");
        OfflineProcessor op = new OfflineProcessor();
        try {
            op.notificationTask(datadir, indexdir, queryfile, outputfile, scalefile);
        } catch (IOException | InterruptedException | ExecutionException | ParseException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            logger.error("entrance:", ex);
            logger.info("client is closed");
        }
    }

}
