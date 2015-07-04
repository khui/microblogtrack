package de.mpii.microblogtrack.task.offlinetrain;

import de.mpii.microblogtrack.userprofiles.TrecQuery;
import de.mpii.microblogtrack.utility.MYConstants;
import de.mpii.microblogtrack.utility.QueryTweetPair;
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
public class ConstructFeatureScaler {

    static Logger logger = Logger.getLogger(ConstructFeatureScaler.class.getName());

    private final TLongObjectMap<Status> tweetidStatus = new TLongObjectHashMap<>();

    private final TLongObjectMap<QueryTweetPair> searchresults = new TLongObjectHashMap<>();

    private final Map<String, double[]> featureMinMax = new HashMap<>();

    private class LabeledTweet extends QueryTweetPair {

        private final int judge;

        private BooleanQuery combinedQuery;

        public LabeledTweet(String queryid, long tweetid, int judge, Status status) {
            super(tweetid, queryid, status);
            this.judge = judge;
        }

        public LabeledTweet(LabeledTweet lt) {
            super(lt);
            this.judge = lt.judge;
        }

        public void setQueryContent(Query query) {
            NumericRangeQuery rangeQuery = NumericRangeQuery.newLongRange(MYConstants.TWEETID, tweetid, tweetid, true, true);
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
     * search the constructed index with <query, label, tweetid> triple to get
     * different retrieval score as features. In particular, read in labeled
     * statuses, together with their judgment, generate list of LabeledTweet to
     * further generate features.
     *
     * @param indexdir
     * @param qrelf
     * @param queryfile
     * @param zipfiles
     * @throws java.io.FileNotFoundException
     * @throws org.apache.lucene.queryparser.classic.ParseException
     * @throws java.lang.ClassNotFoundException
     * @throws java.lang.InstantiationException
     * @throws java.lang.IllegalAccessException
     */
    public void preparedata(String indexdir, String qrelf, String queryfile, String[] zipfiles) throws IOException, FileNotFoundException, ClassNotFoundException, InstantiationException, IllegalAccessException, org.apache.lucene.queryparser.classic.ParseException {
        // read in tweetidStatus
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
        // construct min-max scaler
        computeScaler(searchresults.valueCollection());
        // rescale the features
        for (long tid : searchresults.keys()) {
            searchresults.get(tid).rescaleFeatures(featureMinMax);
        }
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
        queries.putAll(tq.readInQueries(queryfile, analyzer, MYConstants.TWEETSTR));
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
    private void computeScaler(Collection<QueryTweetPair> datapoints) {
        for (QueryTweetPair datapoint : datapoints) {
            updateFeatureMinMax(datapoint);
        }
    }

    /**
     * directly copied from ResultTrackerKMean.updateFeatureMinMax
     *
     * @param qtp
     */
    private void updateFeatureMinMax(QueryTweetPair qtp) {
        TObjectDoubleMap<String> featureValues = qtp.getFeatures();
        double value, min, max;
        for (String feature : featureValues.keySet()) {
            value = featureValues.get(feature);
            if (!featureMinMax.containsKey(feature)) {
                featureMinMax.put(feature, new double[2]);
                featureMinMax.get(feature)[0] = Double.MAX_VALUE;
                featureMinMax.get(feature)[1] = Double.MIN_VALUE;
            }
            min = featureMinMax.get(feature)[0];
            max = featureMinMax.get(feature)[1];
            if (value < min) {
                featureMinMax.get(feature)[0] = value;
            } else if (value > max) {
                featureMinMax.get(feature)[1] = value;
            }

        }
    }

    public static void main(String[] args) throws ParseException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        Options options = new Options();
        options.addOption("d", "datadirectory", true, "data directory");
        options.addOption("i", "indexdirectory", true, "index directory");
        options.addOption("q", "queryfile", true, "query file");
        options.addOption("l", "log4jxml", true, "log4j conf file");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        String outputfile = null, datadirsBYCommas = null, indexdir = null, queryfile = null, log4jconf = null;
        if (cmd.hasOption("i")) {
            indexdir = cmd.getOptionValue("i");
        }
        if (cmd.hasOption("d")) {
            datadirsBYCommas = cmd.getOptionValue("d");
        }
        if (cmd.hasOption("l")) {
            log4jconf = cmd.getOptionValue("l");
        }
        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);
        logger.info("offline process test");
    }

}
