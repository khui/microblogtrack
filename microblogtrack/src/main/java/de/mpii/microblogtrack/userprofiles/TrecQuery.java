package de.mpii.microblogtrack.userprofiles;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.benchmark.quality.QualityQuery;
import org.apache.lucene.benchmark.quality.trec.Trec1MQReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.QueryBuilder;
import de.mpii.microblogtrack.utility.Configuration;
import org.apache.log4j.Logger;
import org.apache.lucene.search.BooleanClause;

/**
 * based on org.apache.lucene.benchmark.quality.trec.TrecTopicsReader
 *
 * @author khui
 */
public class TrecQuery {

    static Logger logger = Logger.getLogger(TrecQuery.class.getName());

    public QualityQuery[] readTrecQuery(String queryfile) throws IOException {
        //TrecTopicsReader ttr = new TrecTopicsReader();
        ParseMicroblogQuery ttr = new ParseMicroblogQuery();
        QualityQuery[] queries;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(queryfile))))) {
            queries = ttr.readQueries(br);
            br.close();
        }
        return queries;
    }

    public QualityQuery[] readTrecQueryIntQID(String queryfile) throws IOException {
        //TrecTopicsReader ttr = new TrecTopicsReader();
        ParseMicroblogQuery ttr = new ParseMicroblogQuery();
        QualityQuery[] queries;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(queryfile))))) {
            queries = ttr.readQueriesIntQID(br);
        }
        return queries;
    }

    public QualityQuery[] readMQTrecQuery(String queryfile) throws IOException {
        Trec1MQReader ttr = new Trec1MQReader("topic");
        QualityQuery[] queries = ttr.readQueries(new BufferedReader(new InputStreamReader(new FileInputStream(new File(queryfile)))));
        return queries;
    }

    public Map<String, Query> readInQueries(String queryfile, Analyzer analyzer, String field) throws IOException, ParseException {
        QualityQuery[] qqs = readTrecQuery(queryfile);
        QueryBuilder qb = new QueryBuilder(analyzer);
        String querystr;
        Map<String, Query> res = new HashMap<>();
        for (QualityQuery qq : qqs) {
            querystr = qq.getValue(Configuration.QUERY_STR);
            res.put(qq.getQueryID(), qb.createBooleanQuery(field, querystr));
        }
        logger.info("In total, read in queries: " + res.size());
        return res;
    }

    public Map<String, Map<String, Query>> readFieldQueries(String queryfile, Analyzer analyzer) throws IOException, ParseException {
        QualityQuery[] qqs = readTrecQuery(queryfile);
        QueryBuilder qb = new QueryBuilder(analyzer);
        String querystr;
        Map<String, Map<String, Query>> res = new HashMap<>();
        for (QualityQuery qq : qqs) {
            querystr = qq.getValue(Configuration.QUERY_STR);
            res.put(qq.getQueryID(), new HashMap<>());
            res.get(qq.getQueryID()).put(Configuration.TWEET_CONTENT, qb.createBooleanQuery(Configuration.TWEET_CONTENT, querystr, BooleanClause.Occur.SHOULD));
            res.get(qq.getQueryID()).put(Configuration.TWEET_URL_TITLE, qb.createBooleanQuery(Configuration.TWEET_URL_TITLE, querystr, BooleanClause.Occur.SHOULD));
        }
        logger.info("In total, read in queries: " + res.size());
        return res;
    }
}
