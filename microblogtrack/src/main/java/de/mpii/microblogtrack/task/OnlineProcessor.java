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
import de.mpii.microblogtrack.userprofiles.TrecQuery;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

/**
 * based on com.twitter.hbc.example.Twitter4jSampleStreamExample
 *
 * @author khui
 */
public class OnlineProcessor {

    static Logger logger = Logger.getLogger(OnlineProcessor.class.getName());

    private BasicClient client;

    private final Map<String, Query> queries;

    private final LuceneScorer lscorer;

    private final StatusListener listener = new StatusListener() {

        @Override
        public void onStatus(Status status) {
            try {
                lscorer.write2Index(status);
            } catch (IOException ex) {
                logger.error(ex.getMessage());
            }
        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice sdn) {
        }

        @Override
        public void onTrackLimitationNotice(int limit) {
        }

        @Override
        public void onScrubGeo(long user, long upToStatus) {
        }

        @Override
        public void onStallWarning(StallWarning warning) {
        }

        @Override
        public void onException(Exception e) {
        }
    };

    public OnlineProcessor(String indexdir, String queryfile) throws IOException, ParseException {
        this.lscorer = new LuceneScorer(indexdir);
        TrecQuery tq = new TrecQuery();
        this.queries = tq.readInQueries(queryfile);
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

    public void listen2API(String keydir, int numProcessingThreads, int queuesize, BlockingQueue<QueryTweetPair> qtweetpairs) throws InterruptedException, IOException, ExecutionException {
        BlockingQueue<String> api2indexqueue = new LinkedBlockingQueue<>(queuesize);
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

        lscorer.multiQuerySearch(qtweetpairs, queries);
    }

    public void close() {
        client.stop();
    }

    public static void main(String[] args) throws InterruptedException, IOException, ParseException, ExecutionException {
        org.apache.log4j.PropertyConfigurator.configure("src/main/java/log4j.xml");
        LogManager.getRootLogger().setLevel(Level.INFO);
        String dir = "/home/khui/workspace/javaworkspace/twitter-localdebug";
        String queryfile = "/home/khui/workspace/result/data/query/microblog/11";
        String indexdir = dir + "/index";
        logger.info("start to process");
        //LangFilterLD.loadprofile(dir + "/lang-dect-profile");
        BlockingQueue<QueryTweetPair> querytweetpairs = new LinkedBlockingQueue<>();
        OnlineProcessor op = new OnlineProcessor(indexdir, queryfile);
        op.listen2API(dir + "/twitterkeys", 1, 1000, querytweetpairs);

        // op.retrieveTopTweets(querytweetpairs);
        while (true) {
            QueryTweetPair qtp = querytweetpairs.poll(100, TimeUnit.MILLISECONDS);
            if (qtp == null) {
                //logger.error("we fail to get query tweet pairs in last period");
            } else {
                logger.info(qtp.toString());
                logger.info(querytweetpairs.size());
            }
        }
    }
}
