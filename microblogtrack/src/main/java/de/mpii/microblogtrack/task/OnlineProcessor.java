package de.mpii.microblogtrack.task;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.hbc.twitter4j.Twitter4jStatusClient;
import de.mpii.microblogtrack.component.LuceneScorer;
import de.mpii.microblogtrack.component.PointwiseDecisionMaker;
import de.mpii.microblogtrack.component.predictor.PointwiseScorer;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import de.mpii.microblogtrack.utility.ResultTweetsTracker;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import hbc.twitter4j.HbcT4jListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.classic.ParseException;
import twitter4j.StatusListener;

/**
 * based on com.twitter.hbc.example.Twitter4jSampleStreamExample
 *
 * @author khui
 */
public class OnlineProcessor {

    static Logger logger = Logger.getLogger(OnlineProcessor.class.getName());

    private BasicClient client;

    /**
     * for notification task: 1) search the results and retain top-k per minute
     * per query 2) the tracker keep track all the search results for each
     * query: the centroids, as reference for distance between tweets, and the
     * score distribution to generate relative score 3) the pointwise prediction
     * scores are computed by pointwise scorer 4) the search results are passed
     * to the decision maker through blocking queue 5) the decision maker will
     * re-called each day
     *
     * @param apikeydir
     * @param indexdir
     * @param queryfile
     * @param outfile
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     * @throws java.util.concurrent.ExecutionException
     * @throws org.apache.lucene.queryparser.classic.ParseException
     * @throws java.lang.ClassNotFoundException
     * @throws java.lang.InstantiationException
     * @throws java.lang.IllegalAccessException
     */
    public void notificationTask(String apikeydir, String indexdir, String queryfile, String outfile) throws IOException, InterruptedException, ExecutionException, ParseException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        // communication between lucene search results and pointwise decision maker
        BlockingQueue<QueryTweetPair> querytweetpairs = new LinkedBlockingQueue<>();
        Map<String, ResultTweetsTracker> queryTrackers = new HashMap<>(250);
        LuceneScorer lscorer = new LuceneScorer(indexdir, queryTrackers, new PointwiseScorer());
        receiveStatus(lscorer, apikeydir, 1);
        lscorer.multiQuerySearch(queryfile, querytweetpairs);
        PointwiseDecisionMaker decisionMaker = new PointwiseDecisionMaker(queryTrackers, querytweetpairs, outfile);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(decisionMaker, 5, 300, TimeUnit.MINUTES);
    }

    /**
     * read in multiple api-keys and store in apikayConfBuilder, associating
     * with corresponding time stamp indicating the latest usage
     *
     * @param keydirectory
     * @throws IOException
     */
    private String[] readAPIKey(String keydirectory) throws IOException {
        BufferedReader br;
        String consumerKey = null, consumerSecret = null, accessToken = null, accessTokenSecret = null;
        // key-timestamp records when the key being used most latest
        br = new BufferedReader(new FileReader(new File(keydirectory,
                "key-timestamp")));
        TLongObjectHashMap<String> timestampKeyfile = new TLongObjectHashMap<>();
        TObjectLongHashMap<String> keyfileTimestamp = new TObjectLongHashMap<>();
        while (br.ready()) {
            String line = br.readLine();
            String[] cols = line.split(" ");
            if (cols.length == 2) {
                timestampKeyfile.put(Long.parseLong(cols[1]), cols[0]);
                keyfileTimestamp.put(cols[0], Long.parseLong(cols[1]));
            }
        }
        br.close();
        long[] timestamps = timestampKeyfile.keys();
        Arrays.sort(timestamps);
        String keyfile = timestampKeyfile.get(timestamps[0]);
        br = new BufferedReader(new FileReader(new File(keydirectory,
                keyfile)));
        while (br.ready()) {
            String line = br.readLine();
            String[] cols = line.split("=");
            switch (cols[0]) {
                case "oauth.consumerKey":
                    consumerKey = cols[1];
                    break;
                case "oauth.consumerSecret":
                    consumerSecret = cols[1];
                    break;
                case "oauth.accessToken":
                    accessToken = cols[1];
                    break;
                case "oauth.accessTokenSecret":
                    accessTokenSecret = cols[1];
                    break;
            }
        }
        br.close();
        try (PrintStream ps = new PrintStream(new File(keydirectory, "key-timestamp"))) {
            for (String keyname : keyfileTimestamp.keySet()) {
                if (keyname.equals(keyfile)) {
                    ps.println(keyname + " " + System.currentTimeMillis());
                    continue;
                }
                ps.println(keyname + " " + keyfileTimestamp.get(keyname));
            }
        }
        return new String[]{consumerKey, consumerSecret, accessToken, accessTokenSecret};
    }

    private void receiveStatus(LuceneScorer lscorer, String keydir, int numProcessingThreads) throws InterruptedException, IOException, ExecutionException, ParseException {
        BlockingQueue<String> api2indexqueue = new LinkedBlockingQueue<>();
        StatusListener listener = new HbcT4jListener(lscorer);
        String[] apikey = readAPIKey(keydir);
        String consumerKey = apikey[0];
        String consumerSecret = apikey[1];
        String token = apikey[2];
        String secret = apikey[3];
        // Define our endpoint: By default, delimited=length is set (we need this for our processor)
        // and stall warnings are on.
        StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);

        // Create a new BasicClient. By default gzip is enabled.
        client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(api2indexqueue))
                .build();

        // Create an executor service which will spawn threads to do the actual work of parsing the incoming messages and
        // calling the listeners on each message
        ExecutorService service = Executors.newFixedThreadPool(numProcessingThreads);

        // Wrap our BasicClient with the twitter4j client
        Twitter4jStatusClient t4jClient = new Twitter4jStatusClient(
                client, api2indexqueue, Lists.newArrayList(listener), service);

        // Establish a connection
        t4jClient.connect();
        for (int threads = 0; threads < numProcessingThreads; threads++) {
            t4jClient.process();
        }

    }

    public void close() {
        client.stop();
    }

    public static void main(String[] args) {
        org.apache.log4j.PropertyConfigurator.configure("src/main/java/log4j.xml");
        //("/home/khui/workspace/javaworkspace/log4j.xml");
        //("src/main/java/log4j.xml");
        LogManager.getRootLogger().setLevel(Level.INFO);
        String dir = "/home/khui/workspace/javaworkspace/twitter-localdebug";
        //"/scratch/GW/pool0/khui/result/microblogtrack";
        //"/home/khui/workspace/javaworkspace/twitter-localdebug";
        String queryfile = "/home/khui/workspace/result/data/query/microblog/14";
        //"/GW/D5data-2/khui/microblogtrack/queries/14";
        //"/home/khui/workspace/result/data/query/microblog/14";
        String keydir = dir + "/twitterkeys";
        //"/GW/D5data-2/khui/microblogtrack/apikeys/batchkeys/apikey4-local";
        //dir + "/twitterkeys"
        String indexdir = dir + "/index_1";
        logger.info("Start To Process");
        OnlineProcessor op = new OnlineProcessor();
        try {
            op.notificationTask(keydir, indexdir, queryfile, "");
        } catch (IOException | InterruptedException | ExecutionException | ParseException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            logger.error("entrance:", ex);
            op.close();
            logger.info("client is closed");
        }
    }
}
