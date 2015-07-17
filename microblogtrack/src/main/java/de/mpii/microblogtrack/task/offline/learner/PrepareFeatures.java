package de.mpii.microblogtrack.task.offline.learner;

import de.mpii.microblogtrack.component.ExtractTweetText;
import de.mpii.microblogtrack.component.IndexTracker;
import de.mpii.microblogtrack.component.core.lucene.UniqQuerySearchResult;
import de.mpii.microblogtrack.component.core.lucene.UniqQuerySearcher;
import de.mpii.microblogtrack.task.offline.qe.ExpandQueryWithWiki;
import de.mpii.microblogtrack.userprofiles.TrecQuery;
import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import de.mpii.microblogtrack.utility.Scaler;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.TObjectDoubleMap;
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
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.FSDirectory;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

/**
 *
 * @author khui
 */
public class PrepareFeatures {

    static Logger logger = Logger.getLogger(PrepareFeatures.class.getName());

    private final TLongObjectMap<LabeledTweet> searchresults = new TLongObjectHashMap<>();

    private final String indexdir, qrelf, queryfile, equeryfile;

    private final String[] qrelTweetZipFiles;

    private final Analyzer analyzer;

    private final String scale_file;

    public PrepareFeatures(String indexdir, String scale_file, String qrelf, String queryfile, String equeryfile, String[] zipfiles) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
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

        public LabeledTweet(QueryTweetPair qtp, int judge, int qidint, Status status) {
            super(qtp);
            this.judge = judge;
            this.binaryjudge = (judge > 0 ? 1 : -1);;
            this.qidint = qidint;
            updateStatus(status);
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

        public Map<String, Query> getTypeQuery() {
            return querytypeQuery;
        }

        public Query getQuery(String type) {
            return querytypeQuery.get(type);
        }

        private void updateStatus(Status status) {
            updateUserTweetFeatures(status, null);
        }
    }

    /**
     * graded labels, qid, features index:value
     *
     * @param outdir
     * @throws java.io.IOException
     * @throws org.apache.lucene.queryparser.classic.ParseException
     * @throws java.io.FileNotFoundException
     * @throws java.lang.ClassNotFoundException
     * @throws java.lang.InstantiationException
     * @throws java.lang.IllegalAccessException
     */
    public void printoutDifferentFeatures(String outdir) throws IOException, ParseException, FileNotFoundException, ClassNotFoundException, InstantiationException, IllegalAccessException, ExecutionException {
        int[] full_qid_range = new int[]{0, 226};
        // the field here is the query type: original query, expanded query etc..
        Map<String, Map<String, Query>> qidFieldQuery = prepareQuery(queryfile, equeryfile);
        // collect tweets for train/test, and compute scaler based on the training data
        multiThreadCollectTweets(indexdir, qrelf, qidFieldQuery, qrelTweetZipFiles, full_qid_range, 18, 4);

        TIntObjectMap<int[]> yearQidrange = new TIntObjectHashMap<>();
        yearQidrange.put(11, new int[]{1, 50});
        yearQidrange.put(12, new int[]{51, 110});
        yearQidrange.put(13, new int[]{111, 170});
        yearQidrange.put(14, new int[]{171, 225});

        /**
         * compute and output scalers
         */
        for (String scale_type : new String[]{Configuration.SCALER_MEANSTD, Configuration.SCALER_MINMAX}) {
            //construct min-max scaler
            Map<String, double[]> featureV1V2 = Scaler.computeScalerMultiThread(full_qid_range, searchresults, scale_type, 24);
            //output scalerz
            Scaler.writeoutScaler(scale_file + "." + scale_type, featureV1V2);
            logger.info("scaler has been output to " + scale_file + "." + scale_type);
        }
        ExecutorService printoutservice = Executors.newFixedThreadPool(24);
        for (int year : yearQidrange.keys()) {
            printoutservice.submit(new FeaturePrinter(null, yearQidrange.get(year), outdir + "/" + year + ".raw", null));
            for (String scale_type : new String[]{Configuration.SCALER_MEANSTD, Configuration.SCALER_MINMAX}) {
                printoutservice.submit(new FeaturePrinter(scale_file + "." + scale_type, yearQidrange.get(year), outdir + "/" + year + "." + scale_type, scale_type));
            }
        }
        printoutservice.shutdown();
        while (!printoutservice.isTerminated()) {

        }
        logger.info("Feature print out is done");
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
    private void multiThreadCollectTweets(String indexdir, String qrelf, Map<String, Map<String, Query>> queries, String[] zipfiles, int[] qid_range, int searchthread, int downloadthread) throws IOException, FileNotFoundException, ClassNotFoundException, InstantiationException, IllegalAccessException, org.apache.lucene.queryparser.classic.ParseException, ExecutionException {
        FetchTweet fetchtweet = new FetchTweet(zipfiles);
        ExecutorService uniqqueryExecutor = Executors.newFixedThreadPool(searchthread);
        Executor downloadURLExcutor = Executors.newFixedThreadPool(downloadthread);
        ExtractTweetText textextractor = new ExtractTweetText(Configuration.LUCENE_DOWNLOAD_URL_TIMEOUT);
        int countNoStatus = 0;
        TLongObjectMap<LabeledTweet> labeledtweets = readinQueryQrel(qrelf, queries);
        DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexdir)));
        long tweetid;
        CompletionService<UniqQuerySearchResult> completeservice = new ExecutorCompletionService<>(uniqqueryExecutor);
        int tasknumber = 0;
        for (LabeledTweet ltweet : labeledtweets.valueCollection()) {
            if (fetchtweet.getStatus(ltweet.tweetid) == null) {
                //logger.error(ltweet.tweetid + "  has not been read in for " + ltweet.queryid);
                countNoStatus++;
                continue;
            }
            if (ltweet.qidint > qid_range[1] || ltweet.qidint < qid_range[0]) {
                continue;
            }
            /**
             * submit each query (query or expanded query, on tweetcontent) as a
             * job to completion service
             */

            try {
                /**
                 * we only extract the url title feature for the judged
                 * relevance tweets
                 */
                if (ltweet.binaryjudge > 0) {
                    completeservice.submit(new UniqQuerySearcher(ltweet.getTypeQuery(), ltweet.queryid, indexReader, downloadURLExcutor, fetchtweet, textextractor, null));
                } else {
                    completeservice.submit(new UniqQuerySearcher(ltweet.getTypeQuery(), ltweet.queryid, indexReader, downloadURLExcutor, fetchtweet, null, null));
                }
                tasknumber++;
            } catch (Exception ex) {
                logger.error("", ex);
            }
        }
        /**
         * pick up the returned results as UniqQuerySearchResult
         */
        UniqQuerySearchResult queryranking;
        LabeledTweet ltweet;
        for (int i = 0; i < tasknumber; ++i) {
            try {
                final Future<UniqQuerySearchResult> futureQtpCollection = completeservice.take();
                queryranking = futureQtpCollection.get();
                if (queryranking != null) {
                    for (QueryTweetPair qtp : queryranking.getSearchResults()) {
                        tweetid = qtp.tweetid;
                        if (!labeledtweets.containsKey(tweetid)) {
                            logger.error("labeled tweets map has no: " + tweetid);
                            continue;
                        }
                        ltweet = labeledtweets.get(tweetid);
                        if (!searchresults.containsKey(tweetid)) {
                            searchresults.put(tweetid, new LabeledTweet(qtp, ltweet.judge, ltweet.qidint, fetchtweet.getStatus(tweetid)));
                        }
                    }
                } else {
                    logger.error("queryranking is null.");
                }

            } catch (ExecutionException | InterruptedException ex) {
                logger.error("Write into the queue for DM", ex);
            }
        }
        logger.info("Successfully load tweets for training in total: " + searchresults.size() + ", judged tweets without status: " + countNoStatus);
    }

    private class FetchTweet extends IndexTracker {

        private final TLongObjectMap<Status> tweetidStatus;

        public FetchTweet(String[] zipfiles) {
            this.tweetidStatus = readInStatus(zipfiles);
        }

        @Override
        public Status getStatus(long tweetid) {
            if (tweetidStatus.containsKey(tweetid)) {
                return tweetidStatus.get(tweetid);
            } else {
                return null;
            }
        }
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
     * read in tweetid - status and store in tweetidStatus
     */
    private TLongObjectMap<Status> readInStatus(String[] zipfiles) {
        TLongObjectMap<Status> tweetidStatus = new TLongObjectHashMap<>();
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
        return tweetidStatus;
    }

    /**
     * read in qrel, query file as <query, label, tweetid> triple, thereafter
     * generate queries for lucene
     */
    private TLongObjectMap<LabeledTweet> readinQueryQrel(String qrelf, Map<String, Map<String, Query>> typeQuery) throws FileNotFoundException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, org.apache.lucene.queryparser.classic.ParseException {
        TLongObjectMap<LabeledTweet> labeledtweets = new TLongObjectHashMap<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(qrelf))))) {
            while (br.ready()) {
                String line = br.readLine();
                String[] cols = line.split(" ");
                String queryid = "MB" + String.format("%03d", Integer.parseInt(cols[0]));
                long tweetid = Long.parseLong(cols[2]);
                int label = Integer.parseInt(cols[3]);
                labeledtweets.put(tweetid, new LabeledTweet(queryid, tweetid, label));
            }
            br.close();
        }
        logger.info("Finished: read qrel and generate labeled tweets " + labeledtweets.size());
        for (LabeledTweet ltweet : labeledtweets.valueCollection()) {
            ltweet.setQueryContent(typeQuery.get(ltweet.queryid));
        }
        return labeledtweets;

    }

    private class FeaturePrinter implements Callable<Void> {

        private final String scale_from_file;
        private final String scale_type;
        private final int[] qid_range;
        private final String outfile;

        private FeaturePrinter(String scale_file, int[] qid_range, String outfile, String scale_type) {
            this.scale_from_file = scale_file;
            this.qid_range = qid_range;
            this.outfile = outfile;
            this.scale_type = scale_type;
        }

        private void printScaledFeatures(Map<String, double[]> featureV1V2) throws FileNotFoundException, Exception {
            Collection<LabeledTweet> datapoints = searchresults.valueCollection();
            int count;
            try (PrintStream ps = new PrintStream(outfile)) {
                StringBuilder sb;
                count = 0;
                svm_node[] featureV = null;
                try {
                    for (LabeledTweet lt : datapoints) {
                        if (lt.qidint >= qid_range[0] && lt.qidint <= qid_range[1]) {
                            count++;
                            switch (scale_type) {
                                case Configuration.SCALER_MINMAX:
                                    featureV = lt.vectorizeLibsvmMinMax(featureV1V2);
                                    break;
                                case Configuration.SCALER_MEANSTD:
                                    featureV = lt.vectorizeLibsvmMeanStd(featureV1V2);
                                    break;
                                default:
                                    logger.error(scale_type + " is not available");
                                    break;
                            }
                            if (featureV.length > 0) {
                                int label = lt.binaryjudge;
                                sb = new StringBuilder();
                                sb.append(label).append(" ");
                                sb.append("qid").append(":").append(lt.qidint).append(" ");
                                for (svm_node feature : featureV) {
                                    if (Math.abs(feature.value) >= 0.000001) {
                                        sb.append(feature.index).append(":").append(String.format("%.6f", feature.value)).append(" ");
                                    }
                                }
                                ps.println(sb.toString());
                            }
                        }
                    }
                    ps.close();
                } catch (Exception ex) {
                    logger.error("", ex);
                }
            }
            logger.info("Print out " + scale_type + " features for: " + qid_range[0] + " to " + qid_range[1] + " and printed " + count);
        }

        /**
         * graded label, qid, features
         *
         * @param datapoints
         * @param qidrange
         * @param outfile
         * @throws FileNotFoundException
         * @throws Exception
         */
        private void printRawFeatures() throws FileNotFoundException, Exception {
            Collection<LabeledTweet> datapoints = searchresults.valueCollection();
            TObjectDoubleMap<String> featureV;
            StringBuilder sb;
            String[] featurenames = QueryTweetPair.getFeatureNames();
            int count = 0;
            try (PrintStream ps = new PrintStream(outfile)) {
                for (LabeledTweet lt : datapoints) {
                    if (lt.qidint >= qid_range[0] && lt.qidint <= qid_range[1]) {
                        count++;
                        int label = lt.judge;
                        featureV = lt.getFeatures();
                        sb = new StringBuilder();
                        sb.append(label).append(" ");
                        sb.append("qid").append(":").append(lt.qidint).append(" ");
                        for (int i = 0; i < featurenames.length; i++) {
                            String featurename = featurenames[i];
                            if (featureV.containsKey(featurename)) {
                                if (Math.abs(featureV.get(featurename)) >= 0.000001) {
                                    sb.append(i + 1).append(":").append(String.format("%.6f", featureV.get(featurename))).append(" ");
                                }
                            }
                        }
                        ps.println(sb.toString());
                    }
                }
                ps.close();
            }
            logger.info("Print out raw features for: " + qid_range[0] + " to " + qid_range[1] + " and printed " + count);
        }

        @Override
        public Void call() throws Exception {
            if (scale_from_file != null) {
                Map<String, double[]> featureV1V2 = Scaler.readinScaler(scale_from_file);
                printScaledFeatures(featureV1V2);
            } else {
                printRawFeatures();
            }
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
//        zipfiles = rootdir + "/tweetzip/tweet11-1.zip"; //+ "," + rootdir + "/tweetzip/tweet13-1.zip";
//        modelfile = rootdir + "/model_file/libsvm_model";
//        scalefile = rootdir + "/scale_file/scaler";
//        qrelf = rootdir + "/qrels/test2000";     //fusion";
//        outputdir = rootdir + "/feature_printout";
//        log4jconf = "src/main/java/log4j.xml";

        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);
        logger.info("off-line training: train and test locally:  " + Configuration.RUN_ID);
        PrepareFeatures ptrain = new PrepareFeatures(indexdir, scalefile, qrelf, queryfile, expandqueryfile, zipfiles.split(","));
        ptrain.printoutDifferentFeatures(outputdir);
    }

}
