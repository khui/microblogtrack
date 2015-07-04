package de.mpii.microblogtrack.utility;

import gnu.trove.map.TObjectDoubleMap;
import java.util.List;

/**
 *
 * @author khui
 */
public class LibsvmWrapper {

    public static class LibsvmDatapoint {

    }

    public static class LibsvmDatapoints {

    }

    /**
     * convert the feature map to the svm data point format each conversion is
     * for one data point
     *
     * @param featureValues
     */
    public static void generateDatapoint(TObjectDoubleMap<String> featureValues) {

    }

    /**
     * convert a list of data points to libsvm format input data
     *
     * @param datapoints
     */
    public static void generateDatapoints(List<LibsvmDatapoint> datapoints) {

    }

    /**
     * wrapper for training procedure and output the model
     *
     * @param modelfile
     */
    public static void train(String modelfile) {

    }

    /**
     * wrapper for predicting single data point, note that we require the
     * probability output
     *
     * @param modelfile
     * @param datapoint
     */
    public static void predict(String modelfile, LibsvmDatapoint datapoint) {

    }

    /**
     * wrapper for predicting batch of data points, primarily for test purpose
     *
     * @param modelfile
     * @param datapoints
     */
    public static void predict(String modelfile, LibsvmDatapoints datapoints) {

    }

}
