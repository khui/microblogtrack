package de.mpii.microblogtrack.component.archiver;

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
 * based on
 * com.twitter.hbc.example.SampleStreamExample/Twitter4jSampleStreamExample keep
 * listening the twitter api and put all stream into into the queue
 *
 * @author khui
 */
public abstract class MultiKeysListener implements Callable<Void> {
    
    final Logger logger = Logger.getLogger(MultiKeysListener.class);
    
    protected final BlockingQueue<String> outQueue;
    
    private final String keydirectory;
    
    private String currentKey = "";
    
    protected TObjectLongHashMap<String> apikeyTimestamp = new TObjectLongHashMap<>();
    
    protected HashMap<String, String[]> apikayKeys = new HashMap<>();
    
    public MultiKeysListener(final BlockingQueue<String> outQueue, final String keydirectory) throws IOException {
        this.outQueue = outQueue;
        this.keydirectory = keydirectory;
    }
    
    @Override
    public Void call() throws Exception {
        // recorder for multiple api-keys, for the sake of robustness
        readinAPIConfBuilder(apikeyTimestamp, apikayKeys);
        keepconnecting();
        return null;
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
     * @param apikeyTimestamp
     * @param apikayKeys
     * @throws InterruptedException
     * @throws FileNotFoundException
     */
    protected void updateListener(TObjectLongHashMap<String> apikeyTimestamp, HashMap<String, String[]> apikayKeys) throws InterruptedException,
            FileNotFoundException,
            Exception {
        long currentTime = System.currentTimeMillis();
        long minimumTime = currentTime;
        String[] keywords = null;
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
        try {
            if ((currentTime - minimumTime) <= 15 * 1000) {
                logger.info(currentKey + " sleep for " + (currentTime - minimumTime));
                Thread.sleep(currentTime - minimumTime);
            }
            for (String apikey : apikayKeys.keySet()) {
                if (apikeyTimestamp.get(apikey) <= minimumTime) {
                    keywords = apikayKeys.get(apikey);
                    logger.info(apikey + " is being used to connect twiter API.");
                    currentKey = apikey;
                    listener(keywords[0], keywords[1], keywords[2], keywords[3]);
                    break;
                }
            }
        } catch (Exception ex) {
            logger.error("", ex);
            
        } finally {
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
        }
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
    protected abstract void listener(String consumerKey, String consumerSecret, String token, String secret) throws Exception;
    
    protected abstract void keepconnecting() throws FileNotFoundException, InterruptedException, Exception;
}
