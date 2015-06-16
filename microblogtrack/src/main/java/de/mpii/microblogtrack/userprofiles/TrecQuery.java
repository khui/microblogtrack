package de.mpii.microblogtrack.userprofiles;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    public Map<String, Query> readInQueries(String queryfile) throws IOException, ParseException {
        QualityQuery[] qqs = readTrecQuery(queryfile);
        SimpleQQParser sqqp = new SimpleQQParser(new String[]{"query"}, "tweeturl");
        Map<String, Query> res = new HashMap<>();
        for (QualityQuery qq : qqs) {
            res.put(qq.getQueryID(), sqqp.parse(qq));
        }
        return res;
    }
}
