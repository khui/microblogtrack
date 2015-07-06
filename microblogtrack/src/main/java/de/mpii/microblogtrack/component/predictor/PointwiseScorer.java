package de.mpii.microblogtrack.component.predictor;

import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import org.apache.log4j.Logger;

/**
 * for pointwise prediction, more complicated predictor should extends this
 * class and override the predictor
 *
 * @author khui
 */
public class PointwiseScorer {

    static Logger logger = Logger.getLogger(PointwiseScorer.class);

    public double predictor(QueryTweetPair qtr) {
        String[] retrievalmodels = Configuration.FEATURES_SEMANTIC;
        double scoresum = 0;
        for (String model : retrievalmodels) {
            scoresum += qtr.getFeature(model);
        }
        qtr.setPredictScore(Configuration.PRED_ABSOLUTESCORE, scoresum);
        return scoresum;
    }

}
