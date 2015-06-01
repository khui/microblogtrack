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
import gnu.trove.map.hash.TObjectLongHashMap;
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
import org.apache.log4j.Logger;

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
        for (String apikey : apikayKeys.keySet()) {
            if (apikeyTimestamp.get(apikey) <= minimumTime) {
                String[] keywords = apikayKeys.get(apikey);
                connect(keywords[0], keywords[1], keywords[2], keywords[3]);
                currentKey = apikey;
                break;
            }
        }
        // api-key should be spared for more than 15 min (the length of
        // time window)
        if ((currentTime - minimumTime) <= 15 * 1000) {
            logger.info(currentKey + " sleep for " + (currentTime - minimumTime));
            Thread.sleep(currentTime - minimumTime);
        }
        logger.info(currentKey + " is being used to connect twiter API.");
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

    private void connect(String consumerKey, String consumerSecret, String token, String secret) throws Exception {
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

}
