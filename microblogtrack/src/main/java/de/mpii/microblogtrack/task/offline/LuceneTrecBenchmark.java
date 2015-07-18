package de.mpii.microblogtrack.task.offline;

import de.mpii.microblogtrack.task.offline.qe.ExpandQueryWithWiki;
import de.mpii.microblogtrack.userprofiles.TrecQuery;
import de.mpii.microblogtrack.utility.Configuration;
import gnu.trove.map.TIntObjectMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.benchmark.quality.Judge;
import org.apache.lucene.benchmark.quality.QualityQuery;
import org.apache.lucene.benchmark.quality.QualityQueryParser;
import org.apache.lucene.benchmark.quality.trec.TrecJudge;
import org.apache.lucene.benchmark.quality.utils.SubmissionReport;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.similarities.AfterEffectB;
import org.apache.lucene.search.similarities.AfterEffectL;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BasicModelBE;
import org.apache.lucene.search.similarities.BasicModelIF;
import org.apache.lucene.search.similarities.DFRSimilarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Normalization.NoNormalization;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.QueryBuilder;

/**
 * search the offline index and compare against the labeled qrel to check
 * whether lucene perform good.
 *
 * @author khui
 */
public class LuceneTrecBenchmark {

    static Logger logger = Logger.getLogger(LuceneTrecBenchmark.class.getName());

    private final Analyzer analyzer;

    private final String fieldname;

    private final QualityQuery[] qqs;

    private final QualityQueryParser qqParser;

    private final Judge judge;

    public LuceneTrecBenchmark(Analyzer analyzer, String fieldname, String queryfile, String expandedquery, String qrelfile, int querytopk) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException, ParseException {
        this.analyzer = analyzer;
        this.fieldname = fieldname;
        // prepare queries
        TrecQuery tq = new TrecQuery();
        qqs = tq.readTrecQueryIntQID(queryfile);
        // set the parsing of quality queries into Lucene queries.
        qqParser = new FromFileQueryParser(expandedquery, querytopk);
        // prepare judge, with trec utilities that read from a QRels file
        judge = new TrecJudge(new BufferedReader(new FileReader(new File(qrelfile))));
        // validate topics & judgments match each other
        judge.validateData(qqs, new PrintWriter(System.out));
    }

    public LuceneTrecBenchmark(Analyzer analyzer, String fieldname, String queryfile, String qrelfile) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException, ParseException {
        this.analyzer = analyzer;
        this.fieldname = fieldname;
        // prepare queries
        TrecQuery tq = new TrecQuery();
        qqs = tq.readTrecQueryIntQID(queryfile);
        // set the parsing of quality queries into Lucene queries.
        qqParser = new MultiFieldQueryParser();
        // prepare judge, with trec utilities that read from a QRels file
        judge = new TrecJudge(new BufferedReader(new FileReader(new File(qrelfile))));
        // validate topics & judgments match each other
        judge.validateData(qqs, new PrintWriter(System.out));
    }

    public class MultiFieldQueryParser implements QualityQueryParser {

        @Override
        public Query parse(QualityQuery qq) throws ParseException {
            QueryBuilder qb = new QueryBuilder(analyzer);
            BooleanQuery combine = new BooleanQuery();
            Query tweetcontent = qb.createBooleanQuery(Configuration.TWEET_CONTENT, qq.getValue(Configuration.QUERY_STR));
            tweetcontent.setBoost(1);
            Query querytweettime = NumericRangeQuery.newLongRange(Configuration.TWEET_ID, 0l, Long.valueOf(qq.getValue(Configuration.QUERY_QUERYTWEETTIME)), true, true);
           // Query urltitle = qb.createBooleanQuery(Configuration.TWEET_URL_TITLE, qq.getValue(Configuration.QUERY_STR));
            //tweetcontent.setBoost(0.8f);
            // Query urlcontent = qb.createBooleanQuery(Configuration.TWEET_URL_CONTENT, qq.getValue(Configuration.QUERY_STR));
            // urlcontent.setBoost(0.6f);
            combine.add(tweetcontent, Occur.SHOULD);
            combine.add(querytweettime, Occur.MUST);
            //combine.add(urltitle, Occur.SHOULD);
            //combine.add(urlcontent, Occur.SHOULD);
            return combine;
        }

    }

    public class FromFileQueryParser implements QualityQueryParser {

        private final TIntObjectMap<Query> queries;

        public FromFileQueryParser(String queryfile, int topk) throws IOException {
            Map<String, Float> fieldWeight = new HashMap<>();
            fieldWeight.put(Configuration.TWEET_CONTENT, 1f);
            fieldWeight.put(Configuration.TWEET_URL_TITLE, 0.8f);
            queries = ExpandQueryWithWiki.readExpandedQueriesIntQid(queryfile, fieldWeight, analyzer, topk);
        }

        @Override
        public Query parse(QualityQuery qq) throws ParseException {
            int queryid = Integer.parseInt(qq.getQueryID());
            Query q = queries.get(queryid);
            //logger.info(qq.getValue(Configuration.QUERY_STR) + " " + q.toString());
            return q;
        }

    }

    public void search(String indexdir) throws IOException, ParseException, Exception {
        Directory dir = FSDirectory.open(Paths.get(indexdir));
        DirectoryReader directoryReader = DirectoryReader.open(dir);
        IndexSearcher searcher;

        for (String name : Configuration.FEATURES_RETRIVEMODELS) {
            searcher = new IndexSearcher(directoryReader);
            switch (name) {
                case Configuration.FEATURE_S_TFIDF:
                    break;
                case Configuration.FEATURE_S_BM25:
                    searcher.setSimilarity(new BM25Similarity(Configuration.FEATURE_S_BM25_k1, Configuration.FEATURE_S_BM25_b));
                    break;
                case Configuration.FEATURE_S_LMD:
                    searcher.setSimilarity(new LMDirichletSimilarity(Configuration.FEATURE_S_LMD_mu));
                    break;
                case Configuration.FEATURE_S_LMJM:
                    searcher.setSimilarity(new LMJelinekMercerSimilarity(Configuration.FEATURE_S_LMJM_Lambda));
                    break;
                case Configuration.FEATURE_S_DFR_BE_B:
                    searcher.setSimilarity(new DFRSimilarity(new BasicModelBE(), new AfterEffectB(), new NoNormalization()));
                    break;
                case Configuration.FEATURE_S_DFR_IF_L:
                    searcher.setSimilarity(new DFRSimilarity(new BasicModelIF(), new AfterEffectL(), new NoNormalization()));
                    break;
            }
            trecbenchmark(searcher, name);
        }
    }

    public void trecbenchmark(IndexSearcher searcher, String searchername) throws IOException, Exception {
        // run the benchmark
        QualityBenchmark qrun = new QualityBenchmark(qqs, qqParser, searcher, Configuration.TWEET_ID);
        SubmissionReport submitLog = null;
        QualityStats[] stats = qrun.execute(judge, submitLog, null);
        // print an average sum of the results
        QualityStats avg = QualityStats.average(stats);

        logger.info(searchername + "\tF1@30: " + avg.getF1(30) + "\t" + "P@30: " + avg.getPrecisionAt(30) + "\t" + "R@30: " + avg.getRecallAt(30));
        logger.info(searchername + "\tF1@100: " + avg.getF1(100) + "\t" + "P@100: " + avg.getPrecisionAt(100) + "\t" + "R@100: " + avg.getRecallAt(100));

    }

    public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException, ParseException, Exception {
        String rootdir = "/home/khui/workspace/javaworkspace/twitter-localdebug";
        String indexdir = rootdir + "/tweet2011-nourl-noRT";
        //"/tweet2011-index";
        String queryfile = rootdir + "/queries/";
        String qrelfile = rootdir + "/qrels/";
        String expandedquery = rootdir + "/queries/queryexpansion.res";

//        indexdir = "/GW/D5data-2/khui/microblogtrack/index/tweet2011-nort-url";
//        queryfile = "/GW/D5data-2/khui/microblogtrack/queries/";
//        qrelfile = "/GW/D5data-2/khui/microblogtrack/qrels/";
//        expandedquery = "/scratch/GW/pool0/khui/result/microblogtrack/queryexpansion.res";
        Analyzer analyzer = (Analyzer) Class.forName(Configuration.LUCENE_ANALYZER).newInstance();
        for (String year : new String[]{"11", "12"}) {
            //LuceneTrecBenchmark ltb = new LuceneTrecBenchmark(analyzer, Configuration.TWEET_CONTENT, queryfile + year, expandedquery, qrelfile + year, 8);
            LuceneTrecBenchmark ltb = new LuceneTrecBenchmark(analyzer, Configuration.TWEET_CONTENT, queryfile + year + ".title", qrelfile + year);

            ltb.search(indexdir);
        }
    }

}
