package de.mpii.microblogtrack.task.offline.qe;

import de.mpii.microblogtrack.component.thirdparty.QueryExpansion;
import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.userprofiles.TrecQuery;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
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
import org.apache.lucene.queryparser.simple.SimpleQueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
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

    public static Map<String, Query> readExpandedQueries(String file, Map<String, Float> weights, Analyzer analyzer) throws FileNotFoundException, IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(file))));
        SimpleQueryParser sqp = new SimpleQueryParser(analyzer, weights);
        sqp.setDefaultOperator(Occur.SHOULD);
        Query query;
        Map<String, Query> queries = new HashMap<>();
        while (br.ready()) {
            String line = br.readLine();
            String[] cols = line.split(" ");
            if (cols.length > 1) {
                String qid = cols[0];
                String querystr = line.replace(qid, "");
                query = sqp.parse(querystr);
                queries.put(qid, query);
            }
        }
        return queries;
    }

    public static TIntObjectMap<Query> readExpandedQueriesIntQid(String file, Map<String, Float> weights, Analyzer analyzer, int topk) throws FileNotFoundException, IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(file))));
        SimpleQueryParser sqp = new SimpleQueryParser(analyzer, weights);
        sqp.setDefaultOperator(Occur.SHOULD);
        BooleanQuery query;
        PriorityQueue<ReadInQuery> queue;
        TIntObjectMap<Query> queries = new TIntObjectHashMap<>();
        while (br.ready()) {
            String line = br.readLine();
            String[] cols = line.split(" ");
            if (cols.length > 1) {
                String qid = cols[0];
                queue = new PriorityQueue<>();
                for (int i = 1; i < cols.length; i++) {
                    String[] termWeight = cols[i].split("\\^");
                    queue.add(new ReadInQuery(qid, sqp.parse(termWeight[0]), Float.parseFloat(termWeight[1])));
                }
                while (queue.size() > topk) {
                    queue.poll();
                }
                query = new BooleanQuery();
                for (ReadInQuery q : queue) {
                    query.add(q.query, Occur.SHOULD);
                }
                queries.put(Integer.parseInt(qid.replace("MB", "")), query);
            }
        }
        return queries;
    }

    private static class ReadInQuery implements Comparable<ReadInQuery> {

        public String qid;
        public Query query;
        public float weight;

        public ReadInQuery(String qid, Query query, float weight) {
            this.qid = qid;
            this.query = query;
            this.weight = weight;
            //logger.info(query.toString());
            query.setBoost(weight);
        }

        @Override
        public int compareTo(ReadInQuery o) {
            if (this.weight > o.weight) {
                return 1;
            } else {
                return -1;
            }
        }
    }

    public String queryExpansion(String querystr, int termNum, int docNum) throws IOException, ParseException {
        IndexSearcher searcher = new IndexSearcher(directoryReader);
        searcher.setSimilarity(new LMDirichletSimilarity());
        QueryParser qp = new QueryParser(fieldname, analyzer);
        Query query = qp.parse(querystr);
        ScoreDoc[] hits = searcher.search(query, docNum).scoreDocs;
        QueryExpansion qe = new QueryExpansion(analyzer, searcher, fieldname);
        qe.setParameters(termNum, docNum, alpha, beta, decay);
        String expandedquery = qe.expandQuery2str(querystr, hits);
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
        String expandedQ;
        try (PrintStream ps = new PrintStream(outputf)) {
            for (QualityQuery qq : qqs) {
                String qid = qq.getQueryID();
                String query = qq.getValue("query");
                expandedQ = eqww.queryExpansion(query, 15, 10);
                ps.println(qid + " " + expandedQ);
            }
            ps.close();
        }
    }

}
