package de.mpii.microblogtrack.task.archiver.listener;

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
 * based on
 * com.twitter.hbc.example.SampleStreamExample/Twitter4jSampleStreamExample keep
 * listening the twitter api and put all stream into into the queue
 *
 * @author khui
 */
public class MultiKeysListenerHBC extends MultiKeysListener {

    final Logger logger = Logger.getLogger(MultiKeysListenerHBC.class);

    private BasicClient clientInUse = null;

    public MultiKeysListenerHBC(final BlockingQueue<String> outQueue, final String keydirectory) throws IOException {
        super(outQueue, keydirectory);
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
    @Override
    public void listener(String consumerKey, String consumerSecret, String token, String secret) throws Exception {
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
                .processor(new StringDelimitedProcessor(outQueue))
                .build();
        // Establish a connection
        clientInUse.connect();
    }

    @Override
    protected void keepconnecting() throws FileNotFoundException, InterruptedException, Exception {
        updateListener(apikeyTimestamp, apikayKeys);
        while (!Thread.currentThread().isInterrupted()) {
            if (clientInUse.isDone()) {
                logger.error("Current client connection closed unexpectedly: " + clientInUse.getExitEvent().getMessage());
                updateListener(apikeyTimestamp, apikayKeys);
            }
            Thread.sleep(900 * 1000);
        }
    }
}
