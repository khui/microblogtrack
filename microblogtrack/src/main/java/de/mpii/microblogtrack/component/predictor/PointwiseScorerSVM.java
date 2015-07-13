package de.mpii.microblogtrack.component.predictor;

import de.mpii.microblogtrack.utility.LibsvmWrapper;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import java.io.IOException;
import java.util.Map;
import libsvm.svm_node;
import org.apache.log4j.Logger;

/**
 *
 * @author khui
 */
public class PointwiseScorerSVM implements PointwiseScorer {

    static Logger logger = Logger.getLogger(PointwiseScorerSVM.class);

    private final Map<String, double[]> featureMeanStd;

    public PointwiseScorerSVM(String scalefile, String modelfile) throws IOException {
        this.featureMeanStd = LibsvmWrapper.readScaler(scalefile);
    }

    @Override
    public double predictor(QueryTweetPair qtr) {
        qtr.rescaleFeatures(featureMeanStd);
        svm_node[] vectorLibsvm = qtr.vectorizeLibsvm();
        return 0;
    }

}
