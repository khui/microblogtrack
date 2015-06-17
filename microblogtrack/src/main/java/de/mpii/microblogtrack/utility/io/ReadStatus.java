package de.mpii.microblogtrack.utility.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.CharBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import org.apache.log4j.Logger;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

/**
 *
 * @author khui
 */
public class ReadStatus implements Callable<Void> {
    
    static Logger logger = Logger.getLogger(ReadStatus.class.getName());
    
    private final File directory;
    
    private final BlockingQueue<Status> bq;
    
    public ReadStatus(String gzipdir, BlockingQueue<Status> bq) {
        this.directory = new File(gzipdir);
        this.bq = bq;
    }
    
    @Override
    public Void call() throws IOException, TwitterException, InterruptedException {
        BufferedReader br;
        String jsonStr;
        Status tweet;
        for (File fileEntry : directory.listFiles()) {
            br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(fileEntry))));
            while (br.ready()) {
                jsonStr = br.readLine();
                tweet = TwitterObjectFactory.createStatus(jsonStr);
                boolean success = bq.offer(tweet, 20, TimeUnit.SECONDS);
                if (!success) {
                    logger.error("offer to queue failed");
                }
            }
            br.close();
        }
        return null;
    }
    
}
