package de.mpii.microblogtrack.task.offlinetrain;

import de.mpii.microblogtrack.userprofiles.TrecQuery;
import de.mpii.microblogtrack.utility.LibsvmWrapper;
import de.mpii.microblogtrack.utility.MYConstants;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.math.stat.StatUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.store.FSDirectory;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

/**
 *
 * @author khui
 */
public class PointwiseTrain {

    static Logger logger = Logger.getLogger(PointwiseTrain.class.getName());

    private final TLongObjectMap<Status> tweetidStatus = new TLongObjectHashMap<>();

    private final TLongObjectMap<LabeledTweet> searchresults = new TLongObjectHashMap<>();

    private Map<String, double[]> featureMeanStd = new HashMap<>();

    private final String indexdir, qrelf, queryfile;

    private final String[] qrelTweetZipFiles;

    public class LabeledTweet extends QueryTweetPair {

        public final int judge;

        public final int binaryjudge;

        public final int qidint;

        private BooleanQuery combinedQuery;

        public LabeledTweet(String queryid, long tweetid, int judge, Status status) {
            super(tweetid, queryid, status);
            this.judge = judge;
            this.binaryjudge = (judge > 0 ? 1 : 0);
            this.qidint = Integer.parseInt(queryid.replace("MB", ""));
        }

        public LabeledTweet(LabeledTweet lt) {
            super(lt);
            this.judge = lt.judge;
            this.binaryjudge = lt.binaryjudge;
            this.qidint = lt.qidint;
        }

        public void setQueryContent(Query query) {
            NumericRangeQuery rangeQuery = NumericRangeQuery.newLongRange(MYConstants.TWEET_ID, tweetid, tweetid, true, true);
            combinedQuery = new BooleanQuery();
            combinedQuery.add(query, BooleanClause.Occur.SHOULD);
            combinedQuery.add(rangeQuery, BooleanClause.Occur.MUST);
        }

        public Query getQuery() {
            return combinedQuery;
        }

        public void updateStatus(Status status) {
            this.status = status;
            updateFeatures();
        }
    }

    public PointwiseTrain(String indexdir, String qrelf, String queryfile, String[] zipfiles) {
        this.indexdir = indexdir;
        this.qrelf = qrelf;
        this.queryfile = queryfile;
        this.qrelTweetZipFiles = zipfiles;
    }

    public void search(String model_file, int predict_probability, String scale_file, int[] train_qid_range) throws IOException, FileNotFoundException, ClassNotFoundException, InstantiationException, IllegalAccessException, org.apache.lucene.queryparser.classic.ParseException {
        // collect tweets for train/test, and compute scaler based on the training data
        collectTweets(indexdir, qrelf, queryfile, qrelTweetZipFiles);
        // construct min-max scaler
        computeScaler(searchresults.valueCollection(), featureMeanStd, train_qid_range);
        // output scaler
        LibsvmWrapper.writeScaler(scale_file, featureMeanStd);
        // scale the feature value and train/test by reading in the scale_file
        scaleNTrain(scale_file, model_file, predict_probability, train_qid_range);
    }

    /**
     * search the constructed index with <query, label, tweetid> triple to get
     * different retrieval score as features. In particular, read in labeled
     * statuses, together with their judgment, generate list of LabeledTweet to
     * further generate features.
     *
     * @param indexdir
     * @param qrelf
     * @param queryfile
     * @param scalefile
     * @param zipfiles
     * @param out_model_file
     * @param predict_probability
     * @throws java.io.FileNotFoundException
     * @throws org.apache.lucene.queryparser.classic.ParseException
     * @throws java.lang.ClassNotFoundException
     * @throws java.lang.InstantiationException
     * @throws java.lang.IllegalAccessException
     */
    private void collectTweets(String indexdir, String qrelf, String queryfile, String[] zipfiles) throws IOException, FileNotFoundException, ClassNotFoundException, InstantiationException, IllegalAccessException, org.apache.lucene.queryparser.classic.ParseException {
        readInStatus(zipfiles);
        List<LabeledTweet> labeledtweets = constructQuery(qrelf, queryfile);
        String[] searchModels = MYConstants.FEATURES_SEMANTIC;
        DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexdir)));
        IndexSearcher searcherInUse;
        ScoreDoc[] hits;
        long tweetid;
        for (LabeledTweet ltweet : labeledtweets) {
            if (!tweetidStatus.containsKey(ltweet.tweetid)) {
                logger.error(ltweet.tweetid + "  has not been read in for " + ltweet.queryid);
                continue;
            }
            for (String name : searchModels) {
                searcherInUse = new IndexSearcher(indexReader);
                switch (name) {
                    case MYConstants.FEATURE_TFIDF:
                        break;
                    case MYConstants.FEATURE_BM25:
                        searcherInUse.setSimilarity(new BM25Similarity());
                        break;
                    case MYConstants.FEATURE_LMD:
                        searcherInUse.setSimilarity(new LMDirichletSimilarity());
                        break;
                }
                hits = searcherInUse.search(ltweet.getQuery(), 1).scoreDocs;
                if (hits.length > 0) {
                    for (ScoreDoc hit : hits) {
                        tweetid = ltweet.tweetid;
                        if (!searchresults.containsKey(tweetid)) {
                            ltweet.updateStatus(tweetidStatus.get(tweetid));
                            searchresults.put(tweetid, new LabeledTweet(ltweet));
                        }
                        searchresults.get(tweetid).updateFeatures(name, hit.score);
                    }
                } else {
                    logger.error("we get zero results for " + ltweet.queryid + " " + ltweet.tweetid);
                }
            }
        }

    }

    /**
     * read in tweetid - status and store in tweetidStatus
     */
    private void readInStatus(String[] zipfiles) {
        ZipFile zipf;
        String jsonstr;
        BufferedReader br;
        StringBuilder sb;
        for (String f : zipfiles) {
            if (f.endsWith("zip")) {
                try {
                    zipf = new ZipFile(f);
                    Enumeration<? extends ZipEntry> entries = zipf.entries();
                    while (entries.hasMoreElements()) {
                        ZipEntry ze = (ZipEntry) entries.nextElement();
                        br = new BufferedReader(
                                new InputStreamReader(zipf.getInputStream(ze)));
                        sb = new StringBuilder();
                        while (br.ready()) {
                            sb.append(br.readLine());
                        }
                        jsonstr = sb.toString();
                        Status status = TwitterObjectFactory.createStatus(jsonstr);
                        tweetidStatus.put(status.getId(), TwitterObjectFactory.createStatus(jsonstr));
                        br.close();
                    }
                    zipf.close();
                } catch (IOException | TwitterException ex) {
                    logger.error("readInTweets", ex);
                }
                logger.info("read in " + f + " finished");
            }
        }
        logger.info("In total, we read in " + tweetidStatus.size() + " status.");
    }

    private void scaleNTrain(String scale_file, String out_model_file, int predict_probability, int[] train_qid_range) throws IOException {
        featureMeanStd = LibsvmWrapper.readScaler(scale_file);
        // rescale the features
        for (long tweet : searchresults.keys()) {
            searchresults.get(tweet).rescaleFeatures(featureMeanStd);
        }
        // split train/test data
        LibsvmWrapper svmwrapper = new LibsvmWrapper();
        LibsvmWrapper.LocalTrainTest traintest = svmwrapper.splitTrainTestData(searchresults.valueCollection(), train_qid_range);
        svmwrapper.train_libsvm(traintest, 1, out_model_file, predict_probability);
        svmwrapper.predict_libsvm(traintest, out_model_file, predict_probability);
    }

    /**
     * read in qrel, query file as <query, label, tweetid> triple, thereafter
     * generate queries for lucene
     */
    private List<LabeledTweet> constructQuery(String qrelf, String queryfile) throws FileNotFoundException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, org.apache.lucene.queryparser.classic.ParseException {
        Analyzer analyzer = (Analyzer) Class.forName(MYConstants.LUCENE_TOKENIZER).newInstance();
        List<LabeledTweet> labeledtweets = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(qrelf))))) {
            while (br.ready()) {
                String line = br.readLine();
                String[] cols = line.split(" ");
                String queryid = "MB" + String.format("%03d", Integer.parseInt(cols[0]));
                long tweetid = Long.parseLong(cols[2]);
                int label = Integer.parseInt(cols[3]);
                labeledtweets.add(new LabeledTweet(queryid, tweetid, label, null));
            }
            br.close();
        }
        logger.info("Finished: read qrel and generate labeled tweets " + labeledtweets.size());
        TrecQuery tq = new TrecQuery();
        Map<String, Query> queries = new HashMap<>();
        queries.putAll(tq.readInQueries(queryfile, analyzer, MYConstants.TWEET_CONTENT));
        logger.info("Finished: read query " + queries.size());
        for (LabeledTweet ltweet : labeledtweets) {
            ltweet.setQueryContent(queries.get(ltweet.queryid));
        }
        return labeledtweets;
    }

    /**
     * compute scaler for each feature, and output to the given file. This
     * scaler will be used in both off-line training and online prediction to
     * normalize the features
     *
     * @param outfile
     * @param datapoints
     */
    private void computeScaler(Collection<LabeledTweet> datapoints, Map<String, double[]> featureMeanStd, int[] train_qid_range) {
        Map<String, TDoubleList> featureValues = new HashMap<>();
        for (LabeledTweet datapoint : datapoints) {
            if (datapoint.qidint >= train_qid_range[0] && datapoint.qidint <= train_qid_range[1]) {
                accumulateFeatureValues(datapoint, featureValues, featureMeanStd);
            }
        }
        double mean, std;
        for (String feature : featureValues.keySet()) {
            double[] featurevalues = featureValues.get(feature).toArray();
            if (featurevalues.length > 0) {
                mean = StatUtils.mean(featurevalues);
                std = StatUtils.variance(featurevalues);
                featureMeanStd.put(feature, new double[]{mean, std});
            } else {
                logger.error("feature length is zero for " + feature);
            }
        }
    }

    /**
     * directly copied from ResultTrackerKMean.accumulateFeatureValues
     *
     * @param qtp
     */
    private void accumulateFeatureValues(QueryTweetPair qtp, Map<String, TDoubleList> featureAllVs, Map<String, double[]> featureMeanStd) {
        TObjectDoubleMap<String> featureValues = qtp.getFeatures();
        double value;
        for (String feature : featureValues.keySet()) {
            value = featureValues.get(feature);
            if (!featureMeanStd.containsKey(feature)) {
                featureAllVs.put(feature, new TDoubleArrayList());
            }
            featureAllVs.get(feature).add(value);
        }
    }

    public static void main(String[] args) throws ParseException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, FileNotFoundException, org.apache.lucene.queryparser.classic.ParseException {
        Options options = new Options();
        options.addOption("q", "queryfile", true, "query file");
        options.addOption("j", "labelfile", true, "qrel");
        options.addOption("z", "zipfiles", true, "zipfiles for qrel tweets");
        options.addOption("i", "indexdirectory", true, "index directory");
        options.addOption("m", "modelfile", true, "model file");
        options.addOption("s", "scalefile", true, "scale file");
        options.addOption("l", "log4jxml", true, "log4j conf file");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        String qrelf = null, indexdir = null, log4jconf = null, queryfile = null, zipfiles = null, modelfile = null, scalefile = null;
        if (cmd.hasOption("i")) {
            indexdir = cmd.getOptionValue("i");
        }
        if (cmd.hasOption("q")) {
            queryfile = cmd.getOptionValue("q");
        }
        if (cmd.hasOption("j")) {
            qrelf = cmd.getOptionValue("j");
        }
        if (cmd.hasOption("z")) {
            zipfiles = cmd.getOptionValue("z");
        }
        if (cmd.hasOption("m")) {
            modelfile = cmd.getOptionValue("m");
        }
        if (cmd.hasOption("s")) {
            scalefile = cmd.getOptionValue("s");
        }
        if (cmd.hasOption("l")) {
            log4jconf = cmd.getOptionValue("l");
        }
        /**
         * for local test
         */
        String rootdir = "/home/khui/workspace/javaworkspace/twitter-localdebug";
        indexdir = rootdir + "/index";
        queryfile = rootdir + "/queries/fusion";
        zipfiles = rootdir + "/tweetzip/tweet11-1.zip";
        //+ "," + rootdir + "/tweetzip/tweet13-1.zip";
        modelfile = rootdir + "/model_file/libsvm_model";
        scalefile = rootdir + "/scale_file/scale_meanstd";
        qrelf = rootdir + "/qrels/fusion";
        log4jconf = "src/main/java/log4j.xml";

        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);
        logger.info("off-line training: train and test locally:  " + MYConstants.RUN_ID);
        PointwiseTrain ptrain = new PointwiseTrain(indexdir, qrelf, queryfile, zipfiles.split(","));
        ptrain.search(modelfile, 0, scalefile, new int[]{1, 50});
    }

}
