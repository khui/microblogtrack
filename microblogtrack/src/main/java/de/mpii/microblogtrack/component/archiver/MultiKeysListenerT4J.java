/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.mpii.microblogtrack.component.archiver;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 *
 * @author khui
 */
public class MultiKeysListenerT4J extends MultiKeysListener {

    private TwitterStream currenttwitter;

    public MultiKeysListenerT4J(BlockingQueue<String> outQueue, String keydirectory) throws IOException {
        super(outQueue, keydirectory);
    }

    private class StatusListenerBQ implements StatusListener {

        private final BlockingQueue<String> outQueue;

        public StatusListenerBQ(BlockingQueue<String> outQueue) {
            this.outQueue = outQueue;
        }

        @Override
        public void onStatus(Status status) {
            String rawJSON = TwitterObjectFactory.getRawJSON(status);
            outQueue.offer(rawJSON);
        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
        }

        @Override
        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
        }

        @Override
        public void onException(Exception ex) {
            if (ex instanceof TwitterException) {
                TwitterException tw_ex = (TwitterException) ex;
                logger.error(tw_ex.getErrorMessage());
            } else {
                logger.error(ex);
            }
            try {
                keepconnecting();
            } catch (InterruptedException ex1) {
                Logger.getLogger(MultiKeysListenerT4J.class.getName()).log(Level.SEVERE, null, ex1);
            } catch (Exception ex1) {
                Logger.getLogger(MultiKeysListenerT4J.class.getName()).log(Level.SEVERE, null, ex1);
            }

        }

        @Override
        public void onScrubGeo(long userId, long upToStatusId) {
        }

        @Override
        public void onStallWarning(StallWarning arg0) {
        }
    }

    @Override
    protected void listener(String consumerKey, String consumerSecret, String token, String secret) throws Exception {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthAccessToken(token);
        cb.setOAuthAccessTokenSecret(secret);
        cb.setOAuthConsumerKey(consumerKey);
        cb.setOAuthConsumerSecret(consumerSecret);
        cb.setJSONStoreEnabled(true);
        currenttwitter = new TwitterStreamFactory(cb.build())
                .getInstance();
        StatusListener statuslistener = new StatusListenerBQ(outQueue);
        currenttwitter.addListener(statuslistener);
        currenttwitter.sample();
    }

    @Override
    protected void keepconnecting() throws FileNotFoundException, InterruptedException, Exception {
        updateListener(apikeyTimestamp, apikayKeys);
    }

}
