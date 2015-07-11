package de.mpii.microblogtrack.task.offline.qe;

import de.mpii.microblogtrack.component.thirdparty.QueryExpansion;
import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.userprofiles.TrecQuery;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.benchmark.quality.QualityQuery;
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

    public Query search(String querystr, int termNum, int docNum) throws IOException, ParseException {
        IndexSearcher searcher = new IndexSearcher(directoryReader);
        searcher.setSimilarity(new LMDirichletSimilarity());
        QueryParser qp = new QueryParser(fieldname, analyzer);
        Query query = qp.parse(querystr);
        ScoreDoc[] hits = searcher.search(query, docNum).scoreDocs;
        QueryExpansion qe = new QueryExpansion(analyzer, searcher, fieldname);
        qe.setParameters(termNum, docNum, alpha, beta, decay);
        Query expandedquery = qe.expandQuery(querystr, hits);
        return expandedquery;
    }

    public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException, ParseException, org.apache.commons.cli.ParseException {
        Options options = new Options();
        options.addOption("o", "outfile", true, "output file");
        options.addOption("d", "dataORkeydirectory", true, "data/api key directory");
        options.addOption("i", "indexdirectory", true, "index directory");
        options.addOption("q", "queryfile", true, "query file");
        options.addOption("s", "meanstdscalefile", true, "scale parameters for feature normalization");
        options.addOption("l", "log4jxml", true, "log4j conf file");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        String outputf = null, indexdir = null, queryfile = null, log4jconf = null;
        if (cmd.hasOption("o")) {
            outputf = cmd.getOptionValue("o");
        }
        if (cmd.hasOption("l")) {
            log4jconf = cmd.getOptionValue("l");
        }
        if (cmd.hasOption("i")) {
            indexdir = cmd.getOptionValue("i");
        }
        if (cmd.hasOption("q")) {
            queryfile = cmd.getOptionValue("q");
        }

        //for local test
//        log4jconf = "src/main/java/log4j.xml";
//        queryfile = "/home/khui/workspace/javaworkspace/twitter-localdebug/queries/fusion";
//        indexdir = "/home/khui/workspace/result/data/smallwikiindex";
//        outputf = "/home/khui/workspace/javaworkspace/twitter-localdebug/outputdir/queryexpand.test";
        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);
        ExpandQueryWithWiki eqww = new ExpandQueryWithWiki("title", indexdir);
        eqww.setQEParameter(1, 0.8f, 0.1f);
        TrecQuery tq = new TrecQuery();
        QualityQuery[] qqs = tq.readTrecQuery(queryfile);
        StringBuilder sb;
        Query expandedQ;
        PrintStream ps = new PrintStream(outputf);
        for (QualityQuery qq : qqs) {
            String qid = qq.getQueryID();
            String query = qq.getValue("query");
            expandedQ = eqww.search(query, 15, 10);
            sb = new StringBuilder();
            sb.append(qid).append("\t").append(query).append(":\n").append(expandedQ.toString("title"));
            ps.println(sb.toString());
        }

    }

}
