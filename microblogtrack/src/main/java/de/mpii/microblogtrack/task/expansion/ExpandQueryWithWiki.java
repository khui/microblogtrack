package de.mpii.microblogtrack.task.expansion;

import de.mpii.microblogtrack.utility.Configuration;
import java.io.IOException;
import java.nio.file.Paths;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/**
 *
 * @author khui
 */
public class ExpandQueryWithWiki {

    static Logger logger = Logger.getLogger(ExpandQueryWithWiki.class.getName());

    private final Analyzer analyzer;

    private final String fieldname;

    private final DirectoryReader directoryReader;

    private float alpha = 0.8f, beta = 0.4f, decay = 0f;

    public ExpandQueryWithWiki(String fieldname, String indexPath) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
        analyzer = (Analyzer) Class.forName(Configuration.LUCENE_ANALYZER).newInstance();
        this.fieldname = fieldname;
        Directory dir = FSDirectory.open(Paths.get(indexPath));
        directoryReader = DirectoryReader.open(dir);
    }

    public void setQEParameter(float alpha, float beta, float decay) {
        this.alpha = alpha;
        this.beta = beta;
        this.decay = decay;
    }

    public String search(String querystr, int termNum, int docNum) throws IOException, ParseException {
        IndexSearcher searcher = new IndexSearcher(directoryReader);
        searcher.setSimilarity(new LMDirichletSimilarity());
        QueryParser qp = new QueryParser(fieldname, analyzer);
        Query query = qp.parse(querystr);
        ScoreDoc[] hits = searcher.search(query, docNum).scoreDocs;
        QueryExpansion qe = new QueryExpansion(analyzer, searcher, fieldname);
        qe.setParameters(termNum, docNum, alpha, beta, decay);
        Query expandedquery = qe.expandQuery(querystr, hits);
        return expandedquery.toString();
    }

    public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException, ParseException {
        String log4jconf = "src/main/java/log4j.xml";
        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);
        ExpandQueryWithWiki eqww = new ExpandQueryWithWiki("title", "/home/khui/workspace/result/data/smallwikiindex");
        String expandedQ = eqww.search("read novels", 20, 30);
        logger.info(expandedQ);
    }

}
