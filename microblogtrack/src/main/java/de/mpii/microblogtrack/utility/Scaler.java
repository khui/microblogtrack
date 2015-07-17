package de.mpii.microblogtrack.utility;

import de.mpii.microblogtrack.task.offline.learner.PrepareFeatures.LabeledTweet;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.TObjectDoubleMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.math.stat.StatUtils;
import org.apache.log4j.Logger;

/**
 *
 * @author khui
 */
public class Scaler {

    static Logger logger = Logger.getLogger(Scaler.class.getName());

    /**
     * directly copied from ResultTrackerKMean.accumulateFeatureValues
     *
     * @param qtp
     */
    private static void accumulateFeatureValues(QueryTweetPair qtp, Map<String, TDoubleList> featureAllVs) {
        TObjectDoubleMap<String> featureValues = qtp.getFeatures();
        double value;
        for (String feature : featureValues.keySet()) {
            value = featureValues.get(feature);
            if (!featureAllVs.containsKey(feature)) {
                featureAllVs.put(feature, new TDoubleArrayList());
            }
            featureAllVs.get(feature).add(value);
        }
    }

    /**
     * compute scaler for each feature, and output to the given file. This
     * scaler will be used in both off-line training and online prediction to
     * normalize the features
     *
     * @param qid_range
     * @param searchresults
     * @param featureMeanStd
     */
    public static void computeScalerMeanStd(int[] qid_range, TLongObjectMap<LabeledTweet> searchresults, Map<String, double[]> featureMeanStd) {
        Collection<LabeledTweet> datapoints = searchresults.valueCollection();
        Map<String, TDoubleList> featureValues = new HashMap<>();
        for (LabeledTweet datapoint : datapoints) {
            if (datapoint.qidint >= qid_range[0] && datapoint.qidint <= qid_range[1]) {
                accumulateFeatureValues(datapoint, featureValues);
            }
        }
        double mean, std;
        for (String feature : featureValues.keySet()) {
            if (Configuration.FEATURES_NO_SCALE.contains(feature)) {
                continue;
            }
            double[] featurevalues = featureValues.get(feature).toArray();
            if (featurevalues.length > 0) {
                mean = StatUtils.mean(featurevalues);
                std = Math.sqrt(StatUtils.variance(featurevalues));
                featureMeanStd.put(feature, new double[]{mean, std});
            } else {
                logger.error("feature length is zero for " + feature);
            }
        }
    }

    public static Map<String, double[]> computeScalerMultiThread(int[] qid_range, TLongObjectMap<LabeledTweet> searchresults, String task, int threadnum) {
        Map<String, double[]> featureV1V2 = Collections.synchronizedMap(new HashMap<>());
        Collection<LabeledTweet> datapoints = searchresults.valueCollection();
        Map<String, TDoubleList> featureValues = new HashMap<>();
        ExecutorService executor = Executors.newFixedThreadPool(threadnum);
        for (LabeledTweet datapoint : datapoints) {
            if (datapoint.qidint >= qid_range[0] && datapoint.qidint <= qid_range[1]) {
                accumulateFeatureValues(datapoint, featureValues);
            }
        }

        for (final String feature : featureValues.keySet()) {
            if (Configuration.FEATURES_NO_SCALE.contains(feature)) {
                continue;
            }
            final double[] featurevalues = featureValues.get(feature).toArray();
            executor.execute(() -> {
                switch (task) {
                    case "minmax":
                        computeScalerMinMaxUniqFeature(feature, featurevalues, featureV1V2);
                        break;
                    case "meanstd":
                        computeScalerMeanStdUniqFeature(feature, featurevalues, featureV1V2);
                        break;
                    default:
                        logger.error(task + " is not available");
                        break;
                }
            });
        }
        executor.shutdown();
        while (!executor.isTerminated()) {

        }
        logger.info("Scaler computation is done");
        return featureV1V2;
    }

    private static void computeScalerMinMaxUniqFeature(String feature, double[] featurevalues, Map<String, double[]> featureMinMax) {
        double min, max;
        if (featurevalues.length > 0) {
            min = StatUtils.min(featurevalues);
            max = StatUtils.max(featurevalues);
            featureMinMax.put(feature, new double[]{min, max});
        } else {
            logger.error("feature length is zero for " + feature);
        }
    }

    private static void computeScalerMeanStdUniqFeature(String feature, double[] featurevalues, Map<String, double[]> featureMinMax) {
        double mean, std;
        if (featurevalues.length > 0) {
            mean = StatUtils.mean(featurevalues);
            std = Math.sqrt(StatUtils.variance(featurevalues));
            featureMinMax.put(feature, new double[]{mean, std});
        } else {
            logger.error("feature length is zero for " + feature);
        }
    }

    public static Map<String, double[]> readinScaler(String infile) throws FileNotFoundException, IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(infile))));
        Map<String, double[]> featureV1V2 = new HashMap<>();
        String feature;
        // v1: mean or min, v2: std or max
        double v1, v2;
        while (br.ready()) {
            String line = br.readLine();
            String[] cols = line.split(":");
            if (cols.length == 3) {
                feature = cols[0];
                v1 = Double.parseDouble(cols[1]);
                v2 = Double.parseDouble(cols[2]);
                featureV1V2.put(feature, new double[]{v1, v2});
            } else {
                logger.error("input scaler column number is wrong: " + cols.length);
            }
        }
        return featureV1V2;
    }

    /**
     * output the scaler file, generated on training data, used in online
     * prediction to re-scale feature values. each line represents a feature,
     * featurename:mean value (min value) : standard derivation (max value)
     *
     * @param outfile
     * @param featureV1V2
     * @throws FileNotFoundException
     */
    public static void writeoutScaler(String outfile, Map<String, double[]> featureV1V2) throws FileNotFoundException {
        try (PrintStream ps = new PrintStream(outfile)) {
            StringBuilder sb;
            for (String feature : featureV1V2.keySet()) {
                sb = new StringBuilder();
                sb.append(feature).append(":");
                double[] meanstd = featureV1V2.get(feature);
                sb.append(meanstd[0]).append(":").append(meanstd[1]);
                ps.println(sb.toString());
            }
            ps.close();
        }
    }

}
