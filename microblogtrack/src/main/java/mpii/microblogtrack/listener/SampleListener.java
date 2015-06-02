/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mpii.microblogtrack.listener;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.hbc.twitter4j.parser.JSONObjectParser;
import gnu.trove.map.hash.TObjectLongHashMap;
import gnu.trove.set.hash.TLongHashSet;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import twitter4j.JSONException;
import twitter4j.JSONObject;
import twitter4j.JSONObjectType;
import twitter4j.PublicObjectFactory;
import twitter4j.TwitterException;
import twitter4j.conf.ConfigurationBuilder;

/**
 * based on com.twitter.hbc.example.SampleStreamExample
 *
 * @author khui
 */
public class SampleListener implements Callable<Void> {

    final Logger logger = Logger.getLogger(SampleListener.class);

    private final BlockingQueue<String> bqueue;

    private final String keydirectory;

    private BasicClient clientInUse = null;
    // keep tracking the tweetid we received, filter out the duplicate tweets
    private final TLongHashSet tweetids = new TLongHashSet();

    public SampleListener(final BlockingQueue<String> bqueue, final String keydirectory) throws IOException {
        this.bqueue = bqueue;
        this.keydirectory = keydirectory;
    }

    @Override
    public Void call() throws Exception {
        // recorder for multiple api-keys, for the sake of robustness
        TObjectLongHashMap<String> apikeyTimestamp = new TObjectLongHashMap<>();
        HashMap<String, String[]> apikayKeys = new HashMap<>();
        String currentKey = "";
        readinAPIConfBuilder(apikeyTimestamp, apikayKeys);
        currentKey = updateListener(apikeyTimestamp, apikayKeys, currentKey);
        while (true) {
            if (clientInUse.isDone()) {
                logger.error("Current client connection closed unexpectedly: " + clientInUse.getExitEvent().getMessage());
                currentKey = updateListener(apikeyTimestamp, apikayKeys, currentKey);
            }
        }
    }

    /**
     * read in multiple api-keys and store in apikayConfBuilder, associating
     * with corresponding time stamp indicating the latest usage
     *
     * @param keydirectory
     * @param expid
     * @throws IOException
     */
    private void readinAPIConfBuilder(TObjectLongHashMap<String> apikeyTimestamp, HashMap<String, String[]> apikayKeys) throws IOException {
        BufferedReader br;
        int keynum = 0;
        String consumerKey = null, consumerSecret = null, accessToken = null, accessTokenSecret = null;
        // key-timestamp records when the key being used most latest
        br = new BufferedReader(new FileReader(new File(keydirectory,
                "key-timestamp")));
        while (br.ready()) {
            String line = br.readLine();
            String[] cols = line.split(" ");
            if (cols.length == 2) {
                apikeyTimestamp.put(cols[0], Long.parseLong(cols[1]));
            }
        }
        br.close();
        // read in multiple keys for backup
        for (String keyfile : new File(keydirectory).list()) {
            if (keyfile.equals("key-timestamp")) {
                continue;
            }
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
            apikayKeys.put(keyfile, new String[]{consumerKey, consumerSecret, accessToken, accessTokenSecret});
        }
        logger.info(apikayKeys.size() + " api keys have been read in from " + keydirectory);
    }

    /**
     * pick up the api-key, being spared for longest time and establish the
     * standing connection with twitter api
     *
     * @param statuslistener
     * @throws InterruptedException
     * @throws FileNotFoundException
     */
    private String updateListener(TObjectLongHashMap<String> apikeyTimestamp, HashMap<String, String[]> apikayKeys, String currentKey) throws InterruptedException,
            FileNotFoundException,
            Exception {
        long currentTime = System.currentTimeMillis();
        long minimumTime = currentTime;
        if (apikeyTimestamp.containsKey(currentKey)) {
            apikeyTimestamp.adjustValue(currentKey, currentTime);
        }
        if (apikayKeys.size() > 1) {
            long[] milsecond = apikeyTimestamp.values();
            Arrays.sort(milsecond);
            minimumTime = milsecond[0];
        }
        // api-key should be spared for more than 15 min (the length of
        // time window)
        if ((currentTime - minimumTime) <= 15 * 1000) {
            logger.info(currentKey + " sleep for " + (currentTime - minimumTime));
            Thread.sleep(currentTime - minimumTime);
        }
        for (String apikey : apikayKeys.keySet()) {
            if (apikeyTimestamp.get(apikey) <= minimumTime) {
                String[] keywords = apikayKeys.get(apikey);
                statuslistener(keywords[0], keywords[1], keywords[2], keywords[3]);
                currentKey = apikey;
                logger.info(currentKey + " is being used to connect twiter API.");
                break;
            }
        }
        // update and rewrite the file records the key and the time stamp
        currentTime = System.currentTimeMillis();
        try (PrintStream ps = new PrintStream(
                new File(keydirectory, "key-timestamp"))) {
            for (String apikey : apikeyTimestamp.keys(new String[0])) {
                if (apikey.equals(currentKey)) {
                    ps.println(currentKey + " " + String.valueOf(currentTime));
                    continue;
                }
                ps.println(apikey + " "
                        + String.valueOf(apikeyTimestamp.get(apikey)));
            }
        }
        return currentKey;
    }

    /**
     * receive and put all stream heard from api into the queue including delete
     * msg, status etc..
     *
     * @param consumerKey
     * @param consumerSecret
     * @param token
     * @param secret
     * @throws Exception
     */
    private void rawlistener(String consumerKey, String consumerSecret, String token, String secret) throws Exception {
        // Define our endpoint: By default, delimited=length is set (we need this for our processor)
        // and stall warnings are on.
        StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
        endpoint.stallWarnings(false);
        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
        // Create a new BasicClient. By default gzip is enabled.
        clientInUse = new ClientBuilder()
                .name("sampleClient")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(bqueue))
                .build();
        // Establish a connection
        clientInUse.connect();
    }

    /**
     * only keep recording the status: including the status and the retweet
     *
     * @param consumerKey
     * @param consumerSecret
     * @param token
     * @param secret
     * @throws Exception
     */
    private void statuslistener(String consumerKey, String consumerSecret, String token, String secret) throws Exception {
        // Define our endpoint: By default, delimited=length is set (we need this for our processor)
        // and stall warnings are on.
        StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
        endpoint.stallWarnings(false);
        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
        // Create a new BasicClient. By default gzip is enabled.
        clientInUse = new ClientBuilder()
                .name("sampleClient")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new FilteredStringDelimitedProcessor(bqueue))
                .build();
        // Establish a connection
        clientInUse.connect();
    }

    private class FilteredStringDelimitedProcessor extends StringDelimitedProcessor {

        public FilteredStringDelimitedProcessor(BlockingQueue<String> queue) {
            super(queue);
        }

        /**
         * only put the status into queue
         */
        @Override
        public boolean process() throws IOException, InterruptedException {
            String msg = processNextMessage();
            boolean isInsert = true;
            while (msg == null) {
                msg = processNextMessage();
            }
            try {
                long tweetid = isStatus(msg);
                if (tweetid > 0 && !tweetids.contains(tweetid)) {
                    isInsert = queue.offer(msg, offerTimeoutMillis, TimeUnit.MILLISECONDS);
                    tweetids.add(tweetid);
                    // clear the record per 20 mins roughly
                    if (tweetids.size() >= 50000) {
                        tweetids.clear();
                    }
                }
            } catch (JSONException | TwitterException ex) {
                java.util.logging.Logger.getLogger(SampleListener.class.getName()).log(Level.SEVERE, null, ex);
            }
            return isInsert;
        }

    }

    /**
     * referred com.twitter.hbc.twitter4j.BaseTwitter4jClient given an input
     * stream, check whether it is status or not currently only response to
     * status/restatus
     *
     * @param msg
     * @return
     * @throws JSONException
     * @throws TwitterException
     * @throws IOException
     */
    private long isStatus(String msg) throws JSONException, TwitterException, IOException {
        JSONObject json = new JSONObject(msg);
        JSONObjectType.Type type = JSONObjectType.determine(json);
        PublicObjectFactory factory = new PublicObjectFactory(new ConfigurationBuilder().build());
        long tweetid = -1;
        int statustype = 0;
        switch (type) {
            case STATUS:
                statustype = 1;
                break;
            case LIMIT:
                break;
            case DELETE:
                break;
            case SCRUB_GEO:
                break;
            case DIRECT_MESSAGE:
            case SENDER:
                break;
            case FRIENDS:
                break;
            case FAVORITE:
                break;
            case UNFAVORITE:
                break;
            case FOLLOW:
                break;
            case UNFOLLOW:
                break;
            case USER_LIST_MEMBER_ADDED:
                break;
            case USER_LIST_MEMBER_DELETED:
                break;
            case USER_LIST_SUBSCRIBED:
                break;
            case USER_LIST_UNSUBSCRIBED:
                break;
            case USER_LIST_CREATED:
                break;
            case USER_LIST_UPDATED:
                break;
            case USER_LIST_DESTROYED:
                break;
            case BLOCK:
                break;
            case UNBLOCK:
                break;
            case USER_UPDATE:
                break;
            case DISCONNECTION:
                break;
            case STALL_WARNING:
                break;
            case UNKNOWN:
            default:
                // sole RT?
                if (JSONObjectParser.isRetweetMessage(json)) {
                    statustype = 2;
                    logger.info("RT: " + msg);
                } else if (JSONObjectParser.isControlStreamMessage(json)) {
                } else {
                }
                break;
        }
        switch (statustype) {
            case 1:
                tweetid = factory.createStatus(json).getId();
                break;
            case 2:
                tweetid = factory.createStatus(JSONObjectParser.parseEventTargetObject(json)).getId();
                break;
        }

        return tweetid;
    }

}
