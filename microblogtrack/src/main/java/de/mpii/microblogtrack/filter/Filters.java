package de.mpii.microblogtrack.filter;

import gnu.trove.set.hash.TLongHashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import de.mpii.microblogtrack.utility.UniqTweet;
import org.apache.log4j.Logger;

/**
 * wrap up all filters as another consumer/producer input is the queue of raw
 * tweet stream output is the filtered status
 *
 * @author khui
 */
public class Filters implements Callable<Void> {

    final Logger logger = Logger.getLogger(Filters.class);

    private final BlockingQueue<String> rawmsgQueue;

    private final BlockingQueue<String> filteredQueue;

    // keep tracking the tweetid we received, filter out the duplicate tweets
    private final TLongHashSet tweetids = new TLongHashSet();

    private final int TIMEOUT;

    private final int numProcessingThreads;

    public Filters(final BlockingQueue<String> rawmsgQueue, final BlockingQueue<String> statusstrQueue, int timeout, int numProcessingThreads) {
        this.rawmsgQueue = rawmsgQueue;
        this.filteredQueue = statusstrQueue;
        this.TIMEOUT = timeout;
        this.numProcessingThreads = numProcessingThreads;
    }

    @Override
    public Void call() throws InterruptedException, ExecutionException, TimeoutException {
        // total tweet counts, including non-English tweets etc..
        int totalTweetReceived = 1;
        // parallize the filter procedure, due to some of the filter might be expensive
        ExecutorService executor = Executors.newFixedThreadPool(numProcessingThreads);
        CompletionService<UniqTweet> completionService
                = new ExecutorCompletionService<>(executor);
        // multiple filters
        Filter[] filters = {new StatusFilter(), new LangFilterTW()};

        int numberoftasksOneRound = numProcessingThreads * 10; // larger than numProcessingThreads
        while (true) {
            for (int i = 0; i < numberoftasksOneRound; i++) {
                completionService.submit(
                        new FiltUniqTweet(rawmsgQueue, filters, TIMEOUT));
            }
            for (int i = 0; i < numberoftasksOneRound; i++) {
                UniqTweet unitweet = completionService.take().get(TIMEOUT, TimeUnit.SECONDS);
                if (unitweet == null) {
                    logger.error("Get no tweet within past " + TIMEOUT + " seconds");
                } else if (unitweet.isStatus) {
                    totalTweetReceived++;
                    if (unitweet.isRetained && !tweetids.contains(unitweet.getTweetId())) {
                        filteredQueue.offer(unitweet.getMsg());
                        tweetids.add(unitweet.getTweetId());
                    }
                    if (tweetids.size() % 50000 == 0 && tweetids.size() > 0) {
                        logger.info("total " + totalTweetReceived + ": retained " + tweetids.size());
                        tweetids.clear();
                    }
                }
            }
        }
    }

}
