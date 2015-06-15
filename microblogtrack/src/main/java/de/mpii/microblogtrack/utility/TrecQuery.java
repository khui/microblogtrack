package de.mpii.microblogtrack.utility;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.lucene.benchmark.quality.QualityQuery;
import org.apache.lucene.benchmark.quality.trec.Trec1MQReader;
//import org.apache.lucene.benchmark.quality.trec.TrecTopicsReader;

/**
 * based on org.apache.lucene.benchmark.quality.trec.TrecTopicsReader
 *
 * @author khui
 */
public class TrecQuery {

    private static final String newline = System.getProperty("line.separator");

    /**
     * Read quality queries from trec format topics file.
     *
     * @param reader where queries are read from.
     * @return the result quality queries.
     * @throws IOException if cannot read the queries.
     */
    public QualityQuery[] readQueries(BufferedReader reader) throws IOException {

        ArrayList<QualityQuery> res = new ArrayList<>();
        StringBuilder sb;
        String[] tags = new String[]{"<query>", "<querytime>", "<querytweettime>"};
        String[] tagnames = new String[]{"query", "querytime", "querytweettime"};

        try {
            while (null != (sb = read(reader, "<top>", null, false, false))) {
                HashMap<String, String> fields = new HashMap<>();
                sb = read(reader, "<num>", null, true, false);
                int k = sb.indexOf(":");
                int t = sb.indexOf("</");
                String id = sb.substring(k + 1, t).trim();
                for (int i = 0; i < tags.length; i++) {
                    sb = read(reader, tags[i], null, true, false);
                    if (sb != null && sb.length() > 0) {
                        k = sb.indexOf(">");
                        t = sb.indexOf("</");
                        String content = sb.substring(k + 1, t).trim();
                        fields.put(tagnames[i], content);
                    }
                }
                QualityQuery topic = new QualityQuery(id, fields);
                res.add(topic);
            }
        } finally {
            reader.close();
        }
        // sort result array (by ID) 
        QualityQuery qq[] = res.toArray(new QualityQuery[0]);
        Arrays.sort(qq);
        return qq;
    }

    // read until finding a line that starts with the specified prefix
    private StringBuilder read(BufferedReader reader, String prefix, StringBuilder sb, boolean collectMatchLine, boolean collectAll) throws IOException {
        sb = (sb == null ? new StringBuilder() : sb);
        String sep = "";
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                return null;
            }
            if (line.startsWith(prefix)) {
                if (collectMatchLine) {
                    sb.append(sep + line);
                    sep = newline;
                }
                break;
            }
            if (collectAll) {
                sb.append(sep + line);
                sep = newline;
            }
        }
        //System.out.println("read: "+sb);
        return sb;
    }

    public static QualityQuery[] readTrecQuery(String queryfile) throws IOException {
        //TrecTopicsReader ttr = new TrecTopicsReader();
        TrecQuery ttr = new TrecQuery();
        QualityQuery[] queries;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(queryfile))))) {
            queries = ttr.readQueries(br);
        }
        return queries;
    }

    public static QualityQuery[] readMQTrecQuery(String queryfile) throws IOException {
        Trec1MQReader ttr = new Trec1MQReader("topic");
        QualityQuery[] queries = ttr.readQueries(new BufferedReader(new InputStreamReader(new FileInputStream(new File(queryfile)))));
        return queries;
    }

    public static void main(String[] args) throws IOException {
        String dir = "/home/khui/workspace/result/data/query/microblog";
        String queryfile = new File(dir, "14").toString();
        TrecQuery.readTrecQuery(queryfile);
    }

}
