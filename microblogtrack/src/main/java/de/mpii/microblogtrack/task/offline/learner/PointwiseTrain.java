package de.mpii.microblogtrack.task.offline.learner;

import de.mpii.microblogtrack.utility.LibsvmWrapper;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.log4j.Logger;

/**
 *
 * @author khui
 */
public class PointwiseTrain {

    static Logger logger = Logger.getLogger(PointwiseTrain.class.getName());

//    
//
//
//   
//
//
//
//
//
//    public void search(String model_file, int predict_probability, String scale_file, int[] train_qid_range) throws IOException, FileNotFoundException, ClassNotFoundException, InstantiationException, IllegalAccessException, org.apache.lucene.queryparser.classic.ParseException {
//        // collect tweets for train/test, and compute scaler based on the training data
//        collectTweets(indexdir, qrelf, queryfile, qrelTweetZipFiles, train_qid_range);
//        // construct min-max scaler
//        LibsvmWrapper.computeScaler(train_qid_range, searchresults, featureMeanStd);
//        // output scaler
//        LibsvmWrapper.writeScaler(scale_file, featureMeanStd);
//        // scale the feature value and train/test by reading in the scale_file
//        trainNTest(scale_file, model_file, predict_probability, train_qid_range);
//    }
//
//    public void printoutFeatures(String scale_file, String out_file, int[] qid_range) throws IOException, FileNotFoundException, ClassNotFoundException, InstantiationException, IllegalAccessException, org.apache.lucene.queryparser.classic.ParseException {
//        // collect tweets for train/test, and compute scaler based on the training data
//        collectTweets(indexdir, qrelf, queryfile, qrelTweetZipFiles, qid_range);
//        // construct min-max scaler
//        LibsvmWrapper.computeScaler(qid_range, searchresults, featureMeanStd);
//        // output scaler
//        LibsvmWrapper.writeScaler(scale_file, featureMeanStd);
//        // print out features
//        featurePrinter(scale_file, qid_range, out_file);
//    }
//
//
//
//
//
//    private void trainNTest(String scale_file, String out_model_file, int predict_probability, int[] train_qid_range) throws IOException {
//        featureMeanStd = LibsvmWrapper.readScaler(scale_file);
//        // rescale the features
//        for (long tweet : searchresults.keys()) {
//            searchresults.get(tweet).rescaleFeatures(featureMeanStd);
//        }
//        // split train/test data
//        LibsvmWrapper svmwrapper = new LibsvmWrapper();
//        LibsvmWrapper.LocalTrainTest traintest = svmwrapper.splitTrainTestData(searchresults.valueCollection(), train_qid_range);
//        svmwrapper.train_libsvm(traintest, 1000, out_model_file, predict_probability);
//        svmwrapper.predict_libsvm(traintest, out_model_file, predict_probability);
//    }





    
}
