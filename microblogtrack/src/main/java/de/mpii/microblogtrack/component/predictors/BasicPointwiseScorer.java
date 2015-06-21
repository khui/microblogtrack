package de.mpii.microblogtrack.component.predictors;

import de.mpii.microblogtrack.utility.MYConstants;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

/**
 * for pointwise prediction, more complicated predictor should extends this
 * class and override the predictor
 *
 * @author khui
 */
public class BasicPointwiseScorer implements Callable<Void> {

    static Logger logger = Logger.getLogger(BasicPointwiseScorer.class);

    private final BlockingQueue<QueryTweetPair> querytweetpairs;

    public BasicPointwiseScorer(BlockingQueue<QueryTweetPair> qtp) {
        this.querytweetpairs = qtp;
    }

    public void predictor(QueryTweetPair qtr) {
        String[] retrievalmodels = MYConstants.irModels;
        double scoresum = 0;
        for (String model : retrievalmodels) {
            scoresum += qtr.getFeature(model);
        }
        qtr.setPredictScore(MYConstants.PREDICTSCORE, scoresum);
    }

    @Override
    public Void call() throws InterruptedException {
        QueryTweetPair qtr;
        int time2wait = 1000;
        while (!Thread.interrupted()) {
            qtr = querytweetpairs.poll(time2wait, TimeUnit.MILLISECONDS);
            if (qtr != null) {
                predictor(qtr);
            } else {
                logger.error("fail to get query tweet pairs in past " + time2wait + " milliseconds");
            }
        }
        return null;
    }

}
