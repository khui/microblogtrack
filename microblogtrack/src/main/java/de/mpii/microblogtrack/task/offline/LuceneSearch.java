package de.mpii.microblogtrack.task.offline;

import de.mpii.microblogtrack.component.core.ResultTrackerKMean;
import de.mpii.microblogtrack.component.thirdparty.QueryExpansion;
import de.mpii.microblogtrack.task.offline.qe.ExpandQueryWithWiki;
import de.mpii.microblogtrack.userprofiles.TrecQuery;
import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

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

    private final Map<String, Query> queries;

    public LuceneSearch(String queryfile, String fieldname, String indexPath) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException, ParseException {
        analyzer = (Analyzer) Class.forName(Configuration.LUCENE_ANALYZER).newInstance();
        this.fieldname = fieldname;
        Directory dir = FSDirectory.open(Paths.get(indexPath));
        directoryReader = DirectoryReader.open(dir);
        TrecQuery tq = new TrecQuery();
        queries = tq.readInQueries(queryfile, this.analyzer, Configuration.TWEET_CONTENT);
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
