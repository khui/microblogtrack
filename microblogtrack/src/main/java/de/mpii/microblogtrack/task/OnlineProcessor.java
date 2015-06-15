package de.mpii.microblogtrack.task;

import com.cybozu.labs.langdetect.LangDetectException;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.hbc.twitter4j.Twitter4jStatusClient;
import de.mpii.microblogtrack.component.features.ExtractTweetText;
import de.mpii.microblogtrack.component.features.LuceneScores;
import de.mpii.microblogtrack.component.filter.DuplicateTweet;
import de.mpii.microblogtrack.component.filter.Filter;
import de.mpii.microblogtrack.component.filter.LangFilterLD;
import de.mpii.microblogtrack.userprofiles.TrecQuery;
import gnu.trove.map.TLongDoubleMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.benchmark.quality.QualityQuery;
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

    private final int numProcessingThreads;

    private final BlockingQueue<String> queue;

    private BasicClient client;

    private final Filter langfilter;

    private final DuplicateTweet duplicateTweet;

    private final ExtractTweetText turlexpand;

    private final LuceneScores lscore;

    private final List<Query> queries;

    private final StatusListener listener = new StatusListener() {

        @Override
        public void onStatus(Status status) {
            boolean isEng = langfilter.isRetain(null, null, status);
            if (isEng) {
                boolean isNew = duplicateTweet.isRetain(null, null, status);
                if (isNew) {
                    try {
                        lscore.indexDoc(status);
                    } catch (IOException ex) {
                        Logger.getLogger(OnlineProcessor.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
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

    public OnlineProcessor(int numProcessingThreads, int queuesize, String indexdir, String queryfile) throws LangDetectException, IOException, ParseException {
        this.numProcessingThreads = numProcessingThreads;
        this.queue = new LinkedBlockingQueue<>(queuesize);
        this.langfilter = new LangFilterLD();
        this.duplicateTweet = new DuplicateTweet();
        this.turlexpand = new ExtractTweetText();
        this.lscore = new LuceneScores(indexdir);
        TrecQuery tq = new TrecQuery();
        QualityQuery[] qqs = tq.readTrecQuery(queryfile);
        this.queries = tq.parseQualityQuery(qqs);

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

    public void process(String keydir) throws InterruptedException, IOException {
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
                .processor(new StringDelimitedProcessor(queue))
                .build();

        // Create an executor service which will spawn threads to do the actual work of parsing the incoming messages and
        // calling the listeners on each message
        ExecutorService service = Executors.newFixedThreadPool(numProcessingThreads);

        // Wrap our BasicClient with the twitter4j client
        Twitter4jStatusClient t4jClient = new Twitter4jStatusClient(
                client, queue, Lists.newArrayList(listener), service);

        // Establish a connection
        t4jClient.connect();
        for (int threads = 0; threads < numProcessingThreads; threads++) {
            // This must be called once per processing thread
            t4jClient.process();
        }
    }

    public void close() {
        client.stop();
    }

    public void retrieveTopTweet() throws IOException, InterruptedException, ExecutionException {
        ExecutorService service = Executors.newFixedThreadPool(4);
        Set<Future<TLongDoubleMap>> resultset = new HashSet<>();
        TLongDoubleMap tweetidScore;
        while (!Thread.interrupted()) {
            long[] minmax = duplicateTweet.getTweetIdRange();
            for (Query termquery : queries) {
                LuceneScores.NRTSearch nrtsearch = lscore.new NRTSearch(10, minmax[0], minmax[1], termquery);
                Future<TLongDoubleMap> tids = service.submit(nrtsearch);
                resultset.add(tids);
            }
            for (Future<TLongDoubleMap> future : resultset) {
                tweetidScore = future.get();
            }
            Thread.sleep(60 * 1000);
        }
    }

    public static void main(String[] args) throws LangDetectException, InterruptedException, IOException, ParseException, ExecutionException {
        String dir = "/home/khui/workspace/javaworkspace/twitter-localdebug";
        String queryfile = "/home/khui/workspace/result/data/query/microblog";
        String indexdir = dir + "/index";
        LangFilterLD.loadprofile(dir + "/lang-dect-profile");
        OnlineProcessor op = new OnlineProcessor(2, 1000, indexdir, queryfile);
        op.process(dir + "/twitterkeys");
        op.retrieveTopTweet();
    }
}
