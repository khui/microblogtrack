package de.mpii.microblogtrack.utility.io.printresult;

import java.util.Map;

/**
 *
 * @author khui
 */
public interface ResultPrinter {

    public void println(Map<String, String> fieldnameContent);

    public void close();

}
