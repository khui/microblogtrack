package de.mpii.microblogtrack.component.predictor;

import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import java.io.IOException;

/**
 *
 * @author khui
 */
public class PointwiseScorerCombine extends PointwiseScorerSVM {
    
    private final PointwiseScorer scorePredictor = new PointwiseScorerSumRetrievalScores();
    
    private final double alpha4svmPositive = Configuration.POINTWISE_PREDICTOR_COMBINE_ALPHA;
    
    public PointwiseScorerCombine() throws IOException {
        super();
        logger.info(PointwiseScorerCombine.class.getName() + " are being used");
    }
    
    @Override
    public double predictor(QueryTweetPair qtr) {
        double svmscore = super.predictor(qtr);
        double lucenescore = scorePredictor.predictor(qtr);
        double result;
        if (svmscore <= 0.5) {
            result = svmscore;
        } else {
            result = alpha4svmPositive * svmscore + (1 - alpha4svmPositive) * lucenescore;
        }
        logger.info(svmscore + "\t" + lucenescore + "\t" + result);
        qtr.setPredictScore(Configuration.PRED_ABSOLUTESCORE, result);
        return result;
    }
    
}
