package de.mpii.microblogtrack.userprofiles;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.lucene.benchmark.quality.QualityQuery;

/**
 *
 * @author khui
 */
public class ParseMicroblogQuery {

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

    public QualityQuery[] readQueriesIntQID(BufferedReader reader) throws IOException {

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
                id = String.valueOf(Integer.parseInt(id.replace("MB", "")));
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
}
