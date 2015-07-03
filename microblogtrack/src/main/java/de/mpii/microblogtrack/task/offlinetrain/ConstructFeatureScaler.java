package de.mpii.microblogtrack.task.offlinetrain;

import de.mpii.microblogtrack.userprofiles.TrecQuery;
import de.mpii.microblogtrack.utility.MYConstants;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
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

/**
 *
 * @author khui
 */
public class ConstructFeatureScaler {

    static Logger logger = Logger.getLogger(ConstructFeatureScaler.class.getName());

    private final TLongObjectMap<Status> tweetidStatus = new TLongObjectHashMap<>();

    private class LabeledTweet {

        public String queryid;
        public long tweetid;
        public int judge;

        private BooleanQuery combinedQuery;

        public LabeledTweet(String queryid, long tweetid, int judge) {
            this.queryid = queryid;
            this.tweetid = tweetid;
            this.judge = judge;
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
    }

    /**
     * search the constructed index with <query, label, tweetid> triple to get
     * different retrieval score as features
     */
    private void searchIndex(String indexdir, List<LabeledTweet> labeledtweets) throws IOException {
        String[] searchModels = MYConstants.irModels;
        DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexdir)));
        IndexSearcher searcher = new IndexSearcher(indexReader);

        IndexSearcher searcherInUse;
        ScoreDoc[] hits;
        Document tweet;
        long tweetid;
        for (LabeledTweet ltweet : labeledtweets) {
            for (String name : searchModels) {
                searcherInUse = new IndexSearcher(indexReader);
                switch (name) {
                    case MYConstants.TFIDF:
                        break;
                    case MYConstants.BM25:
                        searcherInUse.setSimilarity(new BM25Similarity());
                        break;
                    case MYConstants.LMD:
                        searcherInUse.setSimilarity(new LMDirichletSimilarity());
                        break;
                }
                hits = searcherInUse.search(ltweet.getQuery(), 1).scoreDocs;
                for (ScoreDoc hit : hits) {
                    tweet = searcherInUse.doc(hit.doc);
//                    if (!searchresults.containsKey(tweetid)) {
//                        searchresults.put(tweetid, new QueryTweetPair(tweetid, ltweet.tweetid, tweetidStatus.get(ltweet.tweetid)));
//                    }
//                    searchresults.get(tweetid).updateFeatures(name, hit.score);
                }
            }
        }

    }

    /**
     * read in qrel, query file as <query, label, tweetid> triple, thereafter
     * generate queries for lucene
     */
    private void constructQuery(String qrelf, String queryfile) throws FileNotFoundException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, org.apache.lucene.queryparser.classic.ParseException {
        Analyzer analyzer = (Analyzer) Class.forName(MYConstants.LUCENE_TOKENIZER).newInstance();
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
        TrecQuery tq = new TrecQuery();
        Map<String, Query> queries = new HashMap<>();
        queries.putAll(tq.readInQueries(queryfile, analyzer, MYConstants.TWEETSTR));
        logger.info("Finished: read query " + queries.size());
        for (LabeledTweet ltweet : labeledtweets) {
            ltweet.setQueryContent(queries.get(ltweet.queryid));
        }
    }

    /**
     * compute scaler for each feature, and output to the given file. This
     * scaler will be used in both off-line training and online prediction to
     * normalize the features
     *
     * @param outfile
     * @param datapoints
     */
    private void computeScaler(String outfile, List<QueryTweetPair> datapoints) {

    }

    public static void main(String[] args) throws ParseException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
//        Options options = new Options();
//        options.addOption("d", "datadirectory", true, "data directory");
//        options.addOption("i", "indexdirectory", true, "index directory");
//        options.addOption("q", "queryfile", true, "query file");
//        options.addOption("l", "log4jxml", true, "log4j conf file");
//        CommandLineParser parser = new BasicParser();
//        CommandLine cmd = parser.parse(options, args);
//        String outputfile = null, datadirsBYCommas = null, indexdir = null, queryfile = null, log4jconf = null;
//        if (cmd.hasOption("i")) {
//            indexdir = cmd.getOptionValue("i");
//        }
//        if (cmd.hasOption("d")) {
//            datadirsBYCommas = cmd.getOptionValue("d");
//        }
//        if (cmd.hasOption("l")) {
//            log4jconf = cmd.getOptionValue("l");
//        }
//        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
//        LogManager.getRootLogger().setLevel(Level.INFO);
//        logger.info("offline process test");
        System.out.println("MB" + String.format("%03d", Integer.parseInt("1")));

    }

}
