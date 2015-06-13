package de.mpii.microblogtrack.filter;

import de.mpii.microblogtrack.component.filter.LangFilterLD;
import com.cybozu.labs.langdetect.LangDetectException;
import de.mpii.microblogtrack.archiver.listener.MultiKeysListenerT4J;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.ComparisonFailure;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

/**
 *
 * @author khui
 */
@RunWith(Parameterized.class)
public class LangFilterLDTest {

    private final Status tweet;

    private static int failurecount = 0;

    private static int totalcount = 0;

    public LangFilterLDTest(Status tweet) {
        this.tweet = tweet;
    }

    @Parameters
    public static Iterable<Status[]> generateData() throws InterruptedException, TwitterException, IOException, LangDetectException {
        int TIMEOUT = 60;
        BlockingQueue<String> rawStreamQueue = new LinkedBlockingQueue<>(10000);
        ExecutorService listenerservice = Executors.newSingleThreadExecutor();
        String keydir = "/GW/D5data-2/khui/microblogtrack/apikeys/batchkeys/apikey4";
        listenerservice.submit(new MultiKeysListenerT4J(rawStreamQueue, keydir));
        ArrayList<Status[]> statusTwitterlang = new ArrayList<>();
        while (statusTwitterlang.size() <= 1000000) {
            String json = rawStreamQueue.poll(TIMEOUT, TimeUnit.SECONDS);
            if (json != null) {
                Status tweet = TwitterObjectFactory.createStatus(json);
                if (tweet != null) {
                    if (!tweet.getLang().equals("und")) {
                        statusTwitterlang.add(new Status[]{tweet,});
                    }
                }
            }
        }
        listenerservice.shutdownNow();
        // LangFilterLD.loadprofile();
        return statusTwitterlang;
    }

    /**
     * Test of langdetect method, of class LangFilterLD.
     *
     * @throws twitter4j.TwitterException
     * @throws java.lang.InterruptedException
     */
    @Test
    public void testLangdetect()
            throws TwitterException, InterruptedException {
        LangFilterLD instance = new LangFilterLD();
        Status status = this.tweet;
        String text = status.getText();
        String result = instance.langdetect(null, null, status);
        String tweetlang = status.getLang();
        if (tweetlang.equals("en") || result.equals("en")) {
            totalcount++;
            try {
                assertEquals(text, tweetlang, result);
            } catch (ComparisonFailure cf) {
                failurecount++;
                System.err.println(failurecount + "/" + totalcount + ":\t" + cf.getMessage());
            }

        }
        if (totalcount != 0 && totalcount % 1000 == 0) {
            System.out.println(failurecount + "/" + totalcount);
        }
    }
}
