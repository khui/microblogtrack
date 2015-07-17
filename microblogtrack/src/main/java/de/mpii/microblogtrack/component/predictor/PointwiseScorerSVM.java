package de.mpii.microblogtrack.component.predictor;

import de.bwaldvogel.liblinear.Feature;
import de.bwaldvogel.liblinear.Linear;
import de.bwaldvogel.liblinear.Model;
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

    private final Map<String, double[]> featureMeanStd;

    private final Model model;

    public PointwiseScorerSVM(String scalefile, String modelfile) throws IOException {
        this.featureMeanStd = Scaler.readinScaler(scalefile);
        this.model = Model.load(new File(modelfile));
    }

    @Override
    public double predictor(QueryTweetPair qtr) {
        Feature[] instance = qtr.vectorizeLiblinearMeanStd(featureMeanStd);
        double prediction = Linear.predict(model, instance);
        return prediction;
    }

}
