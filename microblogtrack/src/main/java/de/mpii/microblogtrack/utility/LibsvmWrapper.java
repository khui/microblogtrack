package de.mpii.microblogtrack.utility;

import de.mpii.microblogtrack.task.offlinetrain.LibsvmTrain.LabeledTweet;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import libsvm.svm_node;
import org.apache.log4j.Logger;

/**
 *
 * @author khui
 */
public class LibsvmWrapper {

    static Logger logger = Logger.getLogger(LibsvmWrapper.class);

    /**
     * output the scaler file, generated on training data, used in online
     * prediction to re-scale feature values. each line represents a feature,
     * featurename:mean value:standard derivation
     *
     * @param outfile
     * @param featureMeanStd
     * @throws FileNotFoundException
     */
    public static void writeScaler(String outfile, Map<String, double[]> featureMeanStd) throws FileNotFoundException {
        try (PrintStream ps = new PrintStream(outfile)) {
            StringBuilder sb;
            for (String feature : featureMeanStd.keySet()) {
                sb = new StringBuilder();
                sb.append(feature).append(":");
                double[] meanstd = featureMeanStd.get(feature);
                sb.append(meanstd[0]).append(":").append(meanstd[1]);
                ps.println(sb.toString());
            }
            ps.close();
        }
    }

    public static Map<String, double[]> readScaler(String infile) throws FileNotFoundException, IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(infile))));
        Map<String, double[]> featureMeanStd = new HashMap<>();
        String feature;
        double mean, std;
        while (br.ready()) {
            String line = br.readLine();
            String[] cols = line.split(":");
            if (cols.length == 3) {
                feature = cols[0];
                mean = Double.parseDouble(cols[1]);
                std = Double.parseDouble(cols[2]);
                featureMeanStd.put(feature, new double[]{mean, std});
            } else {
                logger.error("input scaler column number is wrong: " + cols.length);
            }
        }
        return featureMeanStd;
    }

    /**
     * convert a list of data points to libsvm format input data
     *
     * @param datapoints
     * @return
     */
    public static List<svm_node[][]> splitTrainTestData(List<LabeledTweet> datapoints) {
        List<svm_node[]> traindata = new ArrayList<>();
        List<svm_node[]> testdata = new ArrayList<>();
        return null;

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
    public static void predict(String modelfile, svm_node[] datapoint) {

    }

    /**
     * wrapper for predicting batch of data points, primarily for test purpose
     *
     * @param modelfile
     * @param datapoints
     */
    public static void predict(String modelfile, svm_node[][] datapoints) {

    }

}
