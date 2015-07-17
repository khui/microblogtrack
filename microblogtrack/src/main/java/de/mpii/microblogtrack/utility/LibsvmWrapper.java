package de.mpii.microblogtrack.utility;

import de.mpii.microblogtrack.task.offline.learner.PrepareFeatures.LabeledTweet;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;
import libsvm.svm_parameter;
import libsvm.svm_print_interface;
import libsvm.svm_problem;
import org.apache.commons.math.stat.StatUtils;
import org.apache.log4j.Logger;

/**
 *
 * @author khui
 */
public class LibsvmWrapper {

    static Logger logger = Logger.getLogger(LibsvmWrapper.class);

    public class LocalTrainTest {

        public svm_problem train_prob;

        public List<svm_node[]> testdata;

        public double[] test_true_labels;

        // each item corrsponds to each test data point in testdata, including the probability 
        // prediction
        private final double[][] test_pred_prob;
        // each item corrsponds to each test data point in testdata
        private final double[] test_pred_labels;

        private int TP, TN, FP, FN;

        public LocalTrainTest(List<svm_node[]> traindata, double[] trainlabel, List<svm_node[]> testdata, double[] testlabel) {
            this.train_prob = new svm_problem();
            this.train_prob.l = trainlabel.length;
            this.train_prob.x = traindata.toArray(new svm_node[0][]);
            this.train_prob.y = trainlabel;
            this.testdata = testdata;
            this.test_true_labels = testlabel;
            this.test_pred_prob = new double[testdata.size()][];
            this.test_pred_labels = new double[testdata.size()];
        }

        public void addPredictResult(int index, double[] pred_prob) {
            this.test_pred_prob[index] = pred_prob;
            this.test_pred_labels[index] = pred_prob[0];
        }

        // use the inverse of the label occurences as the class weight
        public TIntDoubleMap classweight() {
            int positivenum = 0;
            double ppercent, npercent;
            double total = test_true_labels.length;
            for (double label : test_true_labels) {
                if (label > 0) {
                    positivenum++;
                }
            }
            int negativenum = test_true_labels.length - positivenum;
            ppercent = positivenum / total;
            npercent = negativenum / total;
            TIntDoubleMap labelWeight = new TIntDoubleHashMap();
            labelWeight.put(1, 1d / ppercent);
            labelWeight.put(-1, 1d / npercent);
            return labelWeight;
        }

        public double getPrecision() {
            return this.TP / (double) (this.TP + this.FP);
        }

        public double getRecall() {
            return this.TP / (double) (this.TP + this.FN);
        }

        public double getAccuracy() {
            return (this.TP + this.TN) / (double) this.test_true_labels.length;
        }

        public double getFScore(double beta) {
            return ((beta * beta + 1) * this.getPrecision() * this.getRecall())
                    / (beta * beta * this.getPrecision() + this.getRecall());
        }

        public double getF1() {
            return this.getFScore(1);
        }

        public void computeTPTNFPFN() {
            for (int i = 0; i < test_true_labels.length; i++) {
                if (test_pred_labels[i] == 1 && test_true_labels[i] == 1) {
                    TP++;
                } else if (test_pred_labels[i] == -1 && test_true_labels[i] == -1) {
                    TN++;
                } else if (test_pred_labels[i] == 1 && test_true_labels[i] == -1) {
                    FP++;
                } else if (test_pred_labels[i] == -1 && test_true_labels[i] == 1) {
                    FN++;
                }
            }
            StringBuilder sb = new StringBuilder();
            sb.append("Total: ").append(test_true_labels.length);
            sb.append(", TP: ").append(TP);
            sb.append(", TN: ").append(TN);
            sb.append(", FP: ").append(FP);
            sb.append(", FN: ").append(FN);
            logger.info(sb.toString());
        }

    }

    /**
     * convert a list of data points to libsvm format input data, according to
     * the input query id range
     *
     * @param datapoints
     * @param qidrange2Train
     * @param qidrange2Test
     * @return
     */
    public LocalTrainTest splitTrainTestData(Collection<LabeledTweet> datapoints, int[] qidrange2Train, int[] qidrange2Test) {
        List<svm_node[]> traindata = new ArrayList<>();
        TDoubleList trainlabel = new TDoubleArrayList();
        List<svm_node[]> testdata = new ArrayList<>();
        TDoubleList testlabel = new TDoubleArrayList();
        svm_node[] features;
        for (LabeledTweet lt : datapoints) {
//            if (lt.qidint >= qidrange2Train[0] && lt.qidint <= qidrange2Train[1]) {
//                features = lt.vectorizeLibsvmMinMax();
//                if (features.length > 0) {
//                    traindata.add(lt.vectorizeLibsvmMinMax());
//                    trainlabel.add(lt.binaryjudge);
//                }
//            } else if (lt.qidint >= qidrange2Test[0] && lt.qidint <= qidrange2Test[1]) {
//                features = lt.vectorizeLibsvmMinMax();
//                if (features.length > 0) {
//                    testdata.add(lt.vectorizeLibsvmMinMax());
//                    testlabel.add(lt.binaryjudge);
//                }
//            }
        }
        logger.info("training data points: " + traindata.size() + "  test data points: " + testdata.size());
        return new LocalTrainTest(traindata, trainlabel.toArray(), testdata, testlabel.toArray());
    }

    private svm_parameter setupParameters() {
        // set up problem parameters
        svm_parameter param = new svm_parameter();

        // default values
        param.svm_type = svm_parameter.C_SVC;
        param.kernel_type = svm_parameter.LINEAR;
        param.cache_size = 1000;
        param.C = 1;
        param.eps = 1e-3;
        param.probability = 1;
        param.nr_weight = 0;
        param.shrinking = 1;
        param.weight_label = new int[0];
        param.weight = new double[0];
        svm.svm_set_print_string_function(new svm_print_interface() {
            @Override
            public void print(String s) {
            }
        });
        return param;
    }

    public LocalTrainTest splitTrainTestData(Collection<LabeledTweet> datapoints, int[] qidrange2Train) {
        return splitTrainTestData(datapoints, qidrange2Train, new int[]{Math.max(qidrange2Train[1] + 1, 171), 225});
    }

    private void do_cross_validation(svm_problem prob, svm_parameter param, int nr_fold) {
        int i;
        int total_correct = 0;
        double[] target = new double[prob.l];
        svm.svm_cross_validation(prob, param, nr_fold, target);
        for (i = 0; i < prob.l; i++) {
            if (target[i] == prob.y[i]) {
                ++total_correct;
            }
        }
        System.out.print("Cross Validation Accuracy = " + 100.0 * total_correct / prob.l + "%\n");
    }

    /**
     * wrapper for training procedure and output the model
     *
     * @param traintestdata
     * @param bestC
     * @param model_file
     * @param predict_probability
     * @throws java.io.IOException
     */
    public void train_libsvm(LocalTrainTest traintestdata, double bestC, String model_file, int predict_probability) throws IOException {
        svm_problem traindatapoints = traintestdata.train_prob;
        svm_parameter param = setupParameters();
        param.C = bestC;
        param.probability = predict_probability;
        TIntDoubleMap label_weight = traintestdata.classweight();
        param.weight_label = label_weight.keys();
        param.weight = new double[]{label_weight.get(param.weight_label[0]), label_weight.get(param.weight_label[1])};
        String error_msg = svm.svm_check_parameter(traindatapoints, param);
        if (error_msg != null) {
            System.err.print("ERROR: " + error_msg + "\n");
            System.exit(1);
        }
        svm_model model = svm.svm_train(traindatapoints, param);
        svm.svm_save_model(model_file, model);
        logger.info("training finished.");
    }

    /**
     * wrapper for predicting single data point, note that we require the
     * probability output
     *
     * @param traintestdata
     * @param model_file
     * @param predict_probability
     * @throws java.io.IOException
     */
    public void predict_libsvm(LocalTrainTest traintestdata, String model_file, int predict_probability) throws IOException {
        logger.info("start test.");
        double[] prob_estimates = null;
        double[] pred_prob;

        svm_model model = svm.svm_load_model(model_file);
        int nr_class = svm.svm_get_nr_class(model);
        if (model == null) {
            System.err.print("can't open model file " + model_file + "\n");
            System.exit(1);
        }
        if (predict_probability == 1) {
            if (svm.svm_check_probability_model(model) == 0) {
                System.err.print("Model does not support probabiliy estimates\n");
                System.exit(1);
            }
        } else {
            if (svm.svm_check_probability_model(model) != 0) {
                logger.info("Model supports probability estimates, but disabled in prediction.\n");
            }
        }

        if (predict_probability == 1) {
            int[] labels = new int[nr_class];
            svm.svm_get_labels(model, labels);
            prob_estimates = new double[nr_class];
        }
        List<svm_node[]> xs = traintestdata.testdata;
        double[] true_labels = traintestdata.test_true_labels;

        for (int i = 0; i < true_labels.length; i++) {
            double v;
            pred_prob = new double[3];
            if (predict_probability == 1) {
                v = svm.svm_predict_probability(model, xs.get(i), prob_estimates);
                pred_prob[0] = v;
                pred_prob[1] = prob_estimates[0];
                pred_prob[2] = prob_estimates[1];
            } else {
                v = svm.svm_predict(model, xs.get(i));
                pred_prob[0] = v;
            }
            traintestdata.addPredictResult(i, pred_prob);
        }
        traintestdata.computeTPTNFPFN();
        logger.info("F1 = " + traintestdata.getF1() + "  Accuracy = " + traintestdata.getAccuracy());
    }
}
