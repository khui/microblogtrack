package de.mpii.microblogtrack.task;

import de.mpii.microblogtrack.component.core.LuceneScorer;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.classic.ParseException;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

/**
 * this class mainly for test sake: simulate the api real-time stream with
 * offline data, so that we may use past year labeled data to test the
 * performance of our notification/e-mail digest task
 *
 * @author khui
 */
public class OfflineProcessor extends Processor {

    static Logger logger = Logger.getLogger(OfflineProcessor.class.getName());

    private class ReadInTweets implements Runnable {

        private final String datadir;

        private final LuceneScorer lscorer;

        public ReadInTweets(LuceneScorer lscorer, String datadir) {
            this.lscorer = lscorer;
            this.datadir = datadir;
        }

        /**
         * instead of listening to the api, we directly read the tweet from
         * files, here we mainly use twitter2011 dataset, which is in zip file,
         * within each of which there is multiple files, and each of them is a
         * tweet
         *
         * @param lscorer
         * @param datadir
         * @param numProcessingThreads
         */
        @Override
        public void run() {
            ZipFile zipf;
            String jsonstr;
            BufferedReader br;
            StringBuilder sb;
            File datasetDir = new File(datadir);
            int inputtweetcount = 0;
            for (File f : datasetDir.listFiles()) {
                if (f.getName().endsWith("zip")) {
                    try {
                        zipf = new ZipFile(f);
                        Enumeration<? extends ZipEntry> entries = zipf.entries();
                        while (entries.hasMoreElements()) {
                            ZipEntry ze = (ZipEntry) entries.nextElement();
                            br = new BufferedReader(
                                    new InputStreamReader(zipf.getInputStream(ze)));
                            sb = new StringBuilder();
                            while (br.ready()) {
                                sb.append(br.readLine());
                            }
                            jsonstr = sb.toString();
                            lscorer.write2Index(TwitterObjectFactory.createStatus(jsonstr));
                            br.close();
                            inputtweetcount++;
                            if (inputtweetcount % 3000 == 0) {
                                Thread.sleep(1000 * 60);
                            }
                        }
                        zipf.close();
                    } catch (IOException | TwitterException | InterruptedException ex) {
                        logger.error("readInTweets", ex);
                    }
                    logger.info("read in " + f.getName() + " finished");
                }
            }
            logger.info("Finished read in zip files");

        }
    }

    @Override
    protected void receiveStatus(LuceneScorer lscorer, String datadir, int numProcessingThreads) {
        Executor excutor = Executors.newSingleThreadExecutor();
        excutor.execute(new ReadInTweets(lscorer, datadir));
    } 
}
