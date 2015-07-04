package de.mpii.microblogtrack.component.predictor;

import de.mpii.microblogtrack.utility.MYConstants;
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
        String[] retrievalmodels = MYConstants.FEATURES_SEMANTIC;
        double scoresum = 0;
        for (String model : retrievalmodels) {
            scoresum += qtr.getFeature(model);
        }
        qtr.setPredictScore(MYConstants.PRED_ABSOLUTESCORE, scoresum);
        return scoresum;
    }

}
