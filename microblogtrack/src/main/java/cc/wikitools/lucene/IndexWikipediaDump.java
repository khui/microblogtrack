/**
 * wiki-tools-lucene: Java package for searching Wikipedia dumps with Lucene
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package cc.wikitools.lucene;

import cc.wikitools.WikipediaXMLDumpInputStream;
import de.mpii.microblogtrack.utility.Configuration;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.wikiclean.WikiClean;
import org.wikiclean.WikiCleanBuilder;

/**
 * slightly modified cc.wikitools.lucene.IndexWikipediaDump in
 * wiki-tools-lucene, catering for our query expansion task
 *
 */
public class IndexWikipediaDump {

    private static final Logger logger = Logger.getLogger(IndexWikipediaDump.class);

    static final FieldType TEXT_OPTIONS = new FieldType();

    static {
        TEXT_OPTIONS.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
        TEXT_OPTIONS.setStored(true);
        TEXT_OPTIONS.setTokenized(true);
    }

    public static enum IndexField {

        ID("id"),
        TITLE("title"),
        TEXT("text");

        public final String name;

        IndexField(String s) {
            name = s;
        }
    };

    public static void constructIndex(String indexPath, String inputPath) throws UnsupportedEncodingException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        int threads = 16;
        PrintStream out = new PrintStream(System.out, true, "UTF-8");
        WikiClean cleaner = new WikiCleanBuilder().withTitle(true).build();
        Directory dir = FSDirectory.open(Paths.get(indexPath));
        // the analyzer should be the same with the runtime analyzer
        IndexWriterConfig iwc = new IndexWriterConfig((Analyzer) Class.forName(Configuration.LUCENE_ANALYZER).newInstance());
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setRAMBufferSizeMB(Configuration.LUCENE_MEM_SIZE);
        IndexWriter writer = new IndexWriter(dir, iwc);
        logger.info("Creating index at " + indexPath);
        logger.info("Indexing with " + threads + " threads");

        long startTime = System.currentTimeMillis();

        try {
            WikipediaXMLDumpInputStream stream = new WikipediaXMLDumpInputStream(inputPath);

            ExecutorService executor = Executors.newFixedThreadPool(threads);
            int cnt = 0;
            String page;
            while ((page = stream.readNext()) != null) {
                String title = cleaner.getTitle(page);

                // These are heuristic specifically for filtering out non-articles in enwiki-20120104.
                if (title.startsWith("Wikipedia:") || title.startsWith("Portal:") || title.startsWith("File:")) {
                    continue;
                }

                if (page.contains("#REDIRECT") || page.contains("#redirect") || page.contains("#Redirect")) {
                    continue;
                }

                Runnable worker = new AddDocumentRunnable(writer, cleaner, page);
                executor.execute(worker);

                cnt++;
                if (cnt % 10000 == 0) {
                    logger.info(cnt + " articles added");
                }

            }

            executor.shutdown();
            // Wait until all threads are finish
            while (!executor.isTerminated()) {
            }

            logger.info("Total of " + cnt + " articles indexed.");

            logger.info("Total elapsed time: " + (System.currentTimeMillis() - startTime) + "ms");
        } catch (Exception ex) {
            logger.error("", ex);
        } finally {
            writer.close();
            dir.close();
            out.close();
        }
    }

    private static class AddDocumentRunnable implements Runnable {

        private final IndexWriter writer;
        private final WikiClean cleaner;
        private final String page;

        AddDocumentRunnable(IndexWriter writer, WikiClean cleaner, String page) {
            this.writer = writer;
            this.cleaner = cleaner;
            this.page = page;
        }

        @Override
        public void run() {
            Document doc = new Document();
            doc.add(new IntField(IndexField.ID.name, Integer.parseInt(cleaner.getId(page)), Field.Store.YES));
            doc.add(new Field(IndexField.TEXT.name, cleaner.clean(page), TEXT_OPTIONS));
            doc.add(new Field(IndexField.TITLE.name, cleaner.getTitle(page), TEXT_OPTIONS));

            try {
                writer.addDocument(doc);
            } catch (IOException ex) {
                logger.error("", ex);
            }
        }
    }

}
