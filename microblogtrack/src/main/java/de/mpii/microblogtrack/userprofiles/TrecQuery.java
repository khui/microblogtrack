package de.mpii.microblogtrack.userprofiles;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.benchmark.quality.QualityQuery;
import org.apache.lucene.benchmark.quality.trec.Trec1MQReader;
import org.apache.lucene.benchmark.quality.utils.SimpleQQParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
//import org.apache.lucene.benchmark.quality.trec.TrecTopicsReader;

/**
 * based on org.apache.lucene.benchmark.quality.trec.TrecTopicsReader
 *
 * @author khui
 */
public class TrecQuery {

    public QualityQuery[] readTrecQuery(String queryfile) throws IOException {
        //TrecTopicsReader ttr = new TrecTopicsReader();
        ParseMicroblogQuery ttr = new ParseMicroblogQuery();
        QualityQuery[] queries;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(queryfile))))) {
            queries = ttr.readQueries(br);
        }
        return queries;
    }

    public QualityQuery[] readMQTrecQuery(String queryfile) throws IOException {
        Trec1MQReader ttr = new Trec1MQReader("topic");
        QualityQuery[] queries = ttr.readQueries(new BufferedReader(new InputStreamReader(new FileInputStream(new File(queryfile)))));
        return queries;
    }

    public List<Query> parseQualityQuery(QualityQuery[] qqs) throws ParseException {
        SimpleQQParser sqqp = new SimpleQQParser(new String[]{"query"}, "tweeturl");
        Query[] qs = new Query[qqs.length];
        for (int i = 0; i < qqs.length; i++) {
            qs[i] = sqqp.parse(qqs[i]);
        }
        return Arrays.asList(qs);
    }

    public static void main(String[] args) throws IOException, ParseException {
        String dir = "/home/khui/workspace/result/data/query/microblog";
        String queryfile = new File(dir, "14").toString();
        TrecQuery tq = new TrecQuery();
        QualityQuery[] qqs = tq.readTrecQuery(queryfile);
        List<Query> qs = tq.parseQualityQuery(qqs);
        for (Query q : qs) {
            System.out.println(q.toString());
        }
    }

}
