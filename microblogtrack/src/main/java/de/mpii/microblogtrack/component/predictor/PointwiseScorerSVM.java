package de.mpii.microblogtrack.component.predictor;

import de.bwaldvogel.liblinear.Feature;
import de.bwaldvogel.liblinear.Linear;
import de.bwaldvogel.liblinear.Model;
import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import de.mpii.microblogtrack.utility.Scaler;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 *
 * @author khui
 */
public class PointwiseScorerSVM implements PointwiseScorer {

    static Logger logger = Logger.getLogger(PointwiseScorerSVM.class);

    private final Map<String, double[]> featureV1V2;

    private final Model model;

    public PointwiseScorerSVM() throws IOException {
        String scalefile = Configuration.POINTWISE_SVM_SCALE;
        String modelfile = Configuration.POINTWISE_SVM_MODEL;
        this.featureV1V2 = Scaler.readinScaler(scalefile);
        this.model = Model.load(new File(modelfile));
        logger.info(PointwiseScorerSVM.class.getName() + " are being used");

    }

    @Override
    public double predictor(QueryTweetPair qtr) {
        double[] prob_est = new double[2];
        Feature[] instance = qtr.vectorizeLiblinearMinMax(featureV1V2);
        double label = Linear.predictProbability(model, instance, prob_est);
        qtr.setPredictScore(Configuration.PRED_ABSOLUTESCORE, prob_est[0]);
        return prob_est[0];
    }

}
