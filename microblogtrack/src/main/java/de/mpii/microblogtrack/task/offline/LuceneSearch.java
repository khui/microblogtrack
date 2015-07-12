package de.mpii.microblogtrack.task.offline;

import de.mpii.microblogtrack.userprofiles.TrecQuery;
import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.benchmark.quality.Judge;
import org.apache.lucene.benchmark.quality.QualityBenchmark;
import org.apache.lucene.benchmark.quality.QualityQuery;
import org.apache.lucene.benchmark.quality.QualityQueryParser;
import org.apache.lucene.benchmark.quality.QualityStats;
import org.apache.lucene.benchmark.quality.trec.TrecJudge;
import org.apache.lucene.benchmark.quality.trec.TrecTopicsReader;
import org.apache.lucene.benchmark.quality.utils.SimpleQQParser;
import org.apache.lucene.benchmark.quality.utils.SubmissionReport;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.QueryBuilder;

/**
 * search the offline index and compare against the labeled qrel to check
 * whether lucene perform good.
 *
 * @author khui
 */
public class LuceneSearch {

    static Logger logger = Logger.getLogger(LuceneSearch.class.getName());

    private final Analyzer analyzer;

    private final String fieldname;

    private final DirectoryReader directoryReader;

    public LuceneSearch(String fieldname, String indexPath) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException, ParseException {
        analyzer = (Analyzer) Class.forName(Configuration.LUCENE_ANALYZER).newInstance();
        this.fieldname = fieldname;
        Directory dir = FSDirectory.open(Paths.get(indexPath));
        directoryReader = DirectoryReader.open(dir);
    }

    private class MyQueryParser implements QualityQueryParser {

        @Override
        public Query parse(QualityQuery qq) throws ParseException {
            QueryBuilder qb = new QueryBuilder(analyzer);
            return qb.createBooleanQuery(fieldname, qq.getValue(Configuration.QUERY_STR));
        }

    }

    public void trecrun(String queryfile, String qrelfile) throws IOException, Exception {
        PrintWriter resultprinter = new PrintWriter(System.out, true);

        // prepare queries
        TrecQuery tq = new TrecQuery();
        QualityQuery[] qqs = tq.readTrecQueryIntQID(queryfile);

        // set the parsing of quality queries into Lucene queries.
        QualityQueryParser qqParser = new MyQueryParser();

        // prepare judge, with trec utilities that read from a QRels file
        Judge judge = new TrecJudge(new BufferedReader(new FileReader(new File(qrelfile))));

        // validate topics & judgments match each other
        judge.validateData(qqs, resultprinter);

        IndexSearcher searcher = new IndexSearcher(directoryReader);

        searcher.setSimilarity(new BM25Similarity());

        // run the benchmark
        QualityBenchmark qrun = new QualityBenchmark(qqs, qqParser, searcher, Configuration.TWEET_ID);
        SubmissionReport submitLog = null;
        QualityStats[] stats = qrun.execute(judge, submitLog, resultprinter);

        // print an average sum of the results
        QualityStats avg = QualityStats.average(stats);
        avg.log("SUMMARY", 2, resultprinter, "  ");
    }

    public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException, ParseException, Exception {
        String rootdir = "/home/khui/workspace/javaworkspace/twitter-localdebug";
        String indexdir = rootdir + "/tweet2011-index";
        String queryfile = rootdir + "/queries/11";
        String qrelfile = rootdir + "/qrels/11";
        LuceneSearch ls = new LuceneSearch(Configuration.TWEET_CONTENT, indexdir);
        ls.trecrun(queryfile, qrelfile);
    }

    private void search(Query query, int topN) throws IOException, ParseException {
        IndexSearcher searcher = new IndexSearcher(directoryReader);
        searcher.setSimilarity(new LMDirichletSimilarity());
        TLongObjectMap<QueryTweetPair> searchresults = new TLongObjectHashMap<>();
        ScoreDoc[] hits;
        Document tweet;
        long tweetid;
        for (String name : Configuration.FEATURES_SEMANTIC) {
            switch (name) {
                case Configuration.FEATURE_S_TFIDF:
                    break;
                case Configuration.FEATURE_S_BM25:
                    searcher.setSimilarity(new BM25Similarity());
                    break;
                case Configuration.FEATURE_S_LMD:
                    searcher.setSimilarity(new LMDirichletSimilarity());
                    break;
                case Configuration.FEATURE_S_LMJM:
                    searcher.setSimilarity(new LMJelinekMercerSimilarity(Configuration.FEATURE_S_LMJM_Lambda));
                    break;
            }
            hits = searcher.search(query, topN).scoreDocs;
            for (ScoreDoc hit : hits) {
                tweet = searcher.doc(hit.doc);
                tweetid = Long.parseLong(tweet.get(Configuration.TWEET_ID));

            }
        }
    }

}
