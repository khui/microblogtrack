package de.mpii.microblogtrack.task.offline.libsvm;

import de.mpii.microblogtrack.task.offline.qe.ExpandQueryWithWiki;
import de.mpii.microblogtrack.userprofiles.TrecQuery;
import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import de.mpii.microblogtrack.utility.Scaler;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import libsvm.svm_node;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.AfterEffectB;
import org.apache.lucene.search.similarities.AfterEffectL;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BasicModelBE;
import org.apache.lucene.search.similarities.BasicModelIF;
import org.apache.lucene.search.similarities.DFRSimilarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Normalization;
import org.apache.lucene.store.FSDirectory;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

/**
 *
 * @author khui
 */
public class PrintoutFeatures {

    static Logger logger = Logger.getLogger(PrintoutFeatures.class.getName());

    private final TLongObjectMap<Status> tweetidStatus = new TLongObjectHashMap<>();

    private final TLongObjectMap<LabeledTweet> searchresults = new TLongObjectHashMap<>();

    private final String indexdir, qrelf, queryfile, equeryfile;

    private final String[] qrelTweetZipFiles;

    private final Analyzer analyzer;

    private final String scale_file;

    public PrintoutFeatures(String indexdir, String scale_file, String qrelf, String queryfile, String equeryfile, String[] zipfiles) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        this.indexdir = indexdir;
        this.qrelf = qrelf;
        this.queryfile = queryfile;
        this.equeryfile = equeryfile;
        this.qrelTweetZipFiles = zipfiles;
        this.analyzer = (Analyzer) Class.forName(Configuration.LUCENE_ANALYZER).newInstance();
        this.scale_file = scale_file;
    }

    public class LabeledTweet extends QueryTweetPair {

        public final int judge;

        public final int binaryjudge;

        public final int qidint;

        private final Map<String, Query> querytypeQuery = new HashMap<>();

        public LabeledTweet(String queryid, long tweetid, int judge) {
            super(tweetid, queryid);
            this.judge = judge;
            this.binaryjudge = (judge > 0 ? 1 : -1);
            this.qidint = Integer.parseInt(queryid.replace("MB", ""));
        }

        public LabeledTweet(LabeledTweet lt) {
            super(lt);
            this.judge = lt.judge;
            this.binaryjudge = lt.binaryjudge;
            this.qidint = lt.qidint;
        }

        public void setQueryContent(Map<String, Query> qtQuery) {
            BooleanQuery combinedQuery;
            NumericRangeQuery rangeQuery = NumericRangeQuery.newLongRange(Configuration.TWEET_ID, tweetid, tweetid, true, true);
            for (String type : qtQuery.keySet()) {
                combinedQuery = new BooleanQuery();
                combinedQuery.add(rangeQuery, Occur.MUST);
                combinedQuery.add(qtQuery.get(type), Occur.SHOULD);
                querytypeQuery.put(type, combinedQuery);
            }

        }

        public Query getQuery(String type) {
            return querytypeQuery.get(type);
        }

        public void updateStatus(Status status) {
            updateFeatures(status, null);
        }
    }

    private void prepareScaler(String scaletype) throws Exception, FileNotFoundException, ClassNotFoundException, InstantiationException, IllegalAccessException, org.apache.lucene.queryparser.classic.ParseException {
        int[] full_qid_range = new int[]{0, 226};
        Map<String, double[]> featureMinMax;
        // the field here is the query type: original query, expanded query etc..
        Map<String, Map<String, Query>> qidFieldQuery = prepareQuery(queryfile, equeryfile);
        // collect tweets for train/test, and compute scaler based on the training data
        collectTweets(indexdir, qrelf, qidFieldQuery, qrelTweetZipFiles, full_qid_range);
        // construct min-max scaler
        featureMinMax = Scaler.computeScalerMultiThread(full_qid_range, searchresults, scaletype, 16);
        // output scaler
        Scaler.writeoutScaler(scale_file, featureMinMax);
        logger.info("scaler has been output to " + scale_file);
    }

    public Void printFeatures(String outfile_prefix, String scaletype) throws IOException, FileNotFoundException, ClassNotFoundException, InstantiationException, IllegalAccessException, org.apache.lucene.queryparser.classic.ParseException, Exception {
        prepareScaler(scaletype);
        ExecutorService executorservice = Executors.newFixedThreadPool(4);
        TIntObjectMap<int[]> yearQidrange = new TIntObjectHashMap<>();
        yearQidrange.put(11, new int[]{1, 50});
        yearQidrange.put(12, new int[]{51, 110});
        yearQidrange.put(13, new int[]{111, 170});
        yearQidrange.put(14, new int[]{171, 225});
        for (int year : yearQidrange.keys()) {
            executorservice.submit(new FeaturePrinter(this.scale_file, yearQidrange.get(year), outfile_prefix + "." + year));
        }
        executorservice.shutdown();
        while (!executorservice.isTerminated()) {

        }
        logger.info("everything is done");
        return null;
    }

    /**
     * prepare different types of queries according to the query_type from
     * configuration
     */
    private Map<String, Map<String, Query>> prepareQuery(String queryfile, String equeryfile) throws IOException, ParseException {
        TrecQuery tq = new TrecQuery();
        Map<String, Map<String, Query>> qidFieldQuery = new HashMap<>();
        Map<String, Query> qidQuery = null;
        for (String querytype : Configuration.QUERY_TRAIN_TYPES) {
            switch (querytype) {
                case Configuration.QUERY_STR:
                    qidQuery = tq.readInQueries(queryfile, analyzer, Configuration.TWEET_CONTENT);
                    break;
                case Configuration.QUERY_EXPAN:
                    qidQuery = ExpandQueryWithWiki.readExpandedFieldQueriesTrain(equeryfile, analyzer, 10);
                    break;
                case Configuration.QUERY_EXPAN_5:
                    qidQuery = ExpandQueryWithWiki.readExpandedFieldQueriesTrain(equeryfile, analyzer, 5);
                    break;
                case Configuration.QUERY_EXPAN_15:
                    qidQuery = ExpandQueryWithWiki.readExpandedFieldQueriesTrain(equeryfile, analyzer, 15);
                    break;
            }
            for (String qid : qidQuery.keySet()) {
                if (!qidFieldQuery.containsKey(qid)) {
                    qidFieldQuery.put(qid, new HashMap<>());
                }
                qidFieldQuery.get(qid).put(querytype, qidQuery.get(qid));
            }
        }
        logger.info("in total input " + qidFieldQuery.size() + " queries.");
        return qidFieldQuery;
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
    private void collectTweets(String indexdir, String qrelf, Map<String, Map<String, Query>> queries, String[] zipfiles, int[] qid_range) throws IOException, FileNotFoundException, ClassNotFoundException, InstantiationException, IllegalAccessException, org.apache.lucene.queryparser.classic.ParseException {
        readInStatus(zipfiles);
        int countNoStatus = 0;
        List<LabeledTweet> labeledtweets = readinQueryQrel(qrelf, queries);
        String[] searchModels = Configuration.FEATURES_RETRIVEMODELS;
        DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexdir)));
        IndexSearcher searcherInUse;
        ScoreDoc[] hits;
        long tweetid;
        for (LabeledTweet ltweet : labeledtweets) {
            if (!tweetidStatus.containsKey(ltweet.tweetid)) {
                //logger.error(ltweet.tweetid + "  has not been read in for " + ltweet.queryid);
                countNoStatus++;
                continue;
            }
            if (ltweet.qidint > qid_range[1] || ltweet.qidint < qid_range[0]) {
                continue;
            }
            for (String querytype : Configuration.QUERY_TRAIN_TYPES) {
                for (String model : searchModels) {
                    searcherInUse = new IndexSearcher(indexReader);
                    switch (model) {
                        case Configuration.FEATURE_S_TFIDF:
                            break;
                        case Configuration.FEATURE_S_BM25:
                            searcherInUse.setSimilarity(new BM25Similarity(Configuration.FEATURE_S_BM25_k1, Configuration.FEATURE_S_BM25_b));
                            break;
                        case Configuration.FEATURE_S_LMD:
                            searcherInUse.setSimilarity(new LMDirichletSimilarity(Configuration.FEATURE_S_LMD_mu));
                            break;
                        case Configuration.FEATURE_S_LMJM:
                            searcherInUse.setSimilarity(new LMJelinekMercerSimilarity(Configuration.FEATURE_S_LMJM_Lambda));
                            break;
                        case Configuration.FEATURE_S_DFR_BE_B:
                            searcherInUse.setSimilarity(new DFRSimilarity(new BasicModelBE(), new AfterEffectB(), new Normalization.NoNormalization()));
                            break;
                        case Configuration.FEATURE_S_DFR_IF_L:
                            searcherInUse.setSimilarity(new DFRSimilarity(new BasicModelIF(), new AfterEffectL(), new Normalization.NoNormalization()));
                            break;
                    }
                    TopDocs topdocs = null;
                    try {
                        topdocs = searcherInUse.search(ltweet.getQuery(querytype), 1);
                    } catch (NullPointerException ex) {
                        logger.error(ltweet.qidint + "\t" + querytype + "  :   " + ltweet.getQuery(querytype).toString());
                        logger.error("", ex);
                    }
                    if (topdocs == null) {
                        logger.error(tweetidStatus.get(ltweet.tweetid) + " not in lucene index for: " + model + " " + querytype);
                        continue;
                    }
                    hits = topdocs.scoreDocs;
                    if (hits.length > 0) {
                        for (ScoreDoc hit : hits) {
                            tweetid = ltweet.tweetid;
                            if (!searchresults.containsKey(tweetid)) {
                                ltweet.updateStatus(tweetidStatus.get(tweetid));
                                searchresults.put(tweetid, new LabeledTweet(ltweet));
                            }
                            searchresults.get(tweetid).updateFeatures(QueryTweetPair.concatModelQuerytypeFeature(model, querytype), hit.score);
                        }
                    } else {
                        logger.error(ltweet.queryid + " " + ltweet.tweetid + " " + ltweet.judge + " no hit in lucene index");
                    }
                }
            }
        }
        logger.info("Successfully load tweets for training in total: " + searchresults.size() + ", judged tweets without status: " + countNoStatus);
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

    /**
     * read in qrel, query file as <query, label, tweetid> triple, thereafter
     * generate queries for lucene
     */
    private List<LabeledTweet> readinQueryQrel(String qrelf, Map<String, Map<String, Query>> typeQuery) throws FileNotFoundException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, org.apache.lucene.queryparser.classic.ParseException {
        List<LabeledTweet> labeledtweets = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(qrelf))))) {
            while (br.ready()) {
                String line = br.readLine();
                String[] cols = line.split(" ");
                String queryid = "MB" + String.format("%03d", Integer.parseInt(cols[0]));
                long tweetid = Long.parseLong(cols[2]);
                int label = Integer.parseInt(cols[3]);
                labeledtweets.add(new LabeledTweet(queryid, tweetid, label));
            }
            br.close();
        }
        logger.info("Finished: read qrel and generate labeled tweets " + labeledtweets.size());
        for (LabeledTweet ltweet : labeledtweets) {
            ltweet.setQueryContent(typeQuery.get(ltweet.queryid));
        }
        return labeledtweets;
    }

    private class FeaturePrinter implements Callable<Void> {

        private final String scale_file;
        private final int[] qid_range;
        private final String outfile;

        private FeaturePrinter(String scale_file, int[] qid_range, String outfile) {
            this.scale_file = scale_file;
            this.qid_range = qid_range;
            this.outfile = outfile;
        }

        private void printFeatures(Collection<LabeledTweet> datapoints, int[] qidrange, String outfile) throws FileNotFoundException {
            PrintStream ps = new PrintStream(outfile);
            StringBuilder sb;
            for (LabeledTweet lt : datapoints) {
                if (lt.qidint >= qidrange[0] && lt.qidint <= qidrange[1]) {
                    svm_node[] featureV = lt.vectorizeLibsvm();
                    if (featureV.length > 0) {
                        int label = lt.binaryjudge;
                        sb = new StringBuilder();
                        sb.append(label).append(" ");
                        for (svm_node feature : featureV) {
                            sb.append(feature.index).append(":").append(String.format("%.4f", feature.value)).append(" ");
                        }
                        ps.println(sb.toString());
                    }
                }
            }
            ps.close();
            logger.info("Print out finished for: " + qidrange[0] + " to " + qidrange[1]);
        }

        @Override
        public Void call() throws IOException {
            Map<String, double[]> featureMinMax = Scaler.readinScaler(scale_file);
            // rescale the features
            for (long tweet : searchresults.keys()) {
                searchresults.get(tweet).rescaleFeaturesMinMax(featureMinMax);
            }
            String[] featureNames = QueryTweetPair.getFeatureNames();
            StringBuilder sb = new StringBuilder();
            if (featureNames != null) {
                for (String feature : featureNames) {
                    sb.append(feature).append(" ");
                }
            }
            logger.info("feature names: " + sb.toString());
            // printout the features 
            printFeatures(searchresults.valueCollection(), qid_range, outfile);
            return null;
        }
    }

    public static void main(String[] args) throws ParseException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, FileNotFoundException, org.apache.lucene.queryparser.classic.ParseException, org.apache.commons.cli.ParseException, Exception {
        Options options = new Options();
        options.addOption("q", "queryfile", true, "query file");
        options.addOption("e", "expandqueryfile", true, "expanded query file");
        options.addOption("j", "labelfile", true, "qrel");
        options.addOption("z", "zipfiles", true, "zipfiles for qrel tweets");
        options.addOption("i", "indexdirectory", true, "index directory");
        options.addOption("o", "outputdirectory", true, "output directory");
        options.addOption("m", "modelfile", true, "model file");
        options.addOption("s", "scalefile", true, "scale file");
        options.addOption("l", "log4jxml", true, "log4j conf file");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        String qrelf = null, indexdir = null, outputdir = null, log4jconf = null, queryfile = null, zipfiles = null, modelfile = null, scalefile = null, expandqueryfile = null;
        if (cmd.hasOption("i")) {
            indexdir = cmd.getOptionValue("i");
        }
        if (cmd.hasOption("o")) {
            outputdir = cmd.getOptionValue("o");
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
        if (cmd.hasOption("e")) {
            expandqueryfile = cmd.getOptionValue("e");
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
//        String rootdir = "/home/khui/workspace/javaworkspace/twitter-localdebug";
//        indexdir = rootdir + "/index_noRT";
//        queryfile = rootdir + "/queries/fusion.topic";
//        expandqueryfile = rootdir + "/queries/queryexpansion.res";
//        zipfiles = rootdir + "/tweetzip/tweet11-1.zip" + "," + rootdir + "/tweetzip/tweet13-1.zip";
//        modelfile = rootdir + "/model_file/libsvm_model";
//        scalefile = rootdir + "/scale_file/scale_minmax";
//        qrelf = rootdir + "/qrels/fusion";
//        outputdir = rootdir + "/feature_printout";
//        log4jconf = "src/main/java/log4j.xml";
        String scaletype = "MeanStd";
        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);
        logger.info("off-line training: train and test locally:  " + Configuration.RUN_ID);
        PrintoutFeatures ptrain = new PrintoutFeatures(indexdir, scalefile, qrelf, queryfile, expandqueryfile, zipfiles.split(","));
        ptrain.printFeatures(outputdir, scaletype);
    }

}