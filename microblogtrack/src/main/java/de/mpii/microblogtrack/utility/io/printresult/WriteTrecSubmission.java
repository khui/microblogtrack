package de.mpii.microblogtrack.utility.io.printresult;

import de.mpii.microblogtrack.utility.MYConstants;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Map;

/**
 *
 * @author khui
 */
public class WriteTrecSubmission implements ResultPrinter {

    private final PrintStream ps;

    public WriteTrecSubmission(String outFile) throws FileNotFoundException {
        this.ps = new PrintStream(new FileOutputStream(outFile, true));
    }

    @Override
    public void println(Map<String, String> fieldContent) {
        String queryId = fieldContent.get(MYConstants.QUERYID);
        String tweetId = fieldContent.get(MYConstants.TWEETID);
        String rank = fieldContent.get(MYConstants.RES_RANK);
        String methodId = fieldContent.get(MYConstants.RES_RUNINFO);
        String tweetstr = fieldContent.get(MYConstants.TWEETSTR);
        StringBuilder sb = new StringBuilder();
        sb.append(queryId).append(" ");
        sb.append("Q0").append(" ");
        sb.append(tweetId).append(" ");
        sb.append(rank).append(" ");
        sb.append(methodId).append("\t");
        sb.append(tweetstr);
        this.ps.println(sb.toString());
        this.ps.flush();
    }

    @Override
    public void close() {
        this.ps.close();
    }

}
