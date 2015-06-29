package de.mpii.microblogtrack.component.predictor;

import de.mpii.microblogtrack.utility.QueryTweetPair;
import org.apache.log4j.Logger;

/**
 *
 * @author khui
 */
public class PointwiseScorerSVM extends PointwiseScorer {

    static Logger logger = Logger.getLogger(PointwiseScorerSVM.class);

    public PointwiseScorerSVM() {

    }

    @Override
    public double predictor(QueryTweetPair qtr) {
        return 0;
    }

}
