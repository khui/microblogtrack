/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mpii.microblogtrack.task;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import mpii.microblogtrack.consumer.Dump2Files;
import mpii.microblogtrack.listener.SampleListener;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 * based on com.twitter.hbc.example.SampleStreamExample
 *
 * @author khui
 */
public class SampleHbc {

    final Logger logger = Logger.getLogger(SampleHbc.class);

    public void listenDump(String keydir, String outdir, int queueBound) throws IOException {
        // Create an appropriately sized blocking queue
        BlockingQueue<String> queue = new LinkedBlockingQueue<>(queueBound);
        Future listener = Executors.newSingleThreadExecutor().submit(new SampleListener(queue, keydir));
        Future dumper = Executors.newSingleThreadExecutor().submit(new Dump2Files(queue, outdir));
        while (true) {
            if (listener.isDone()) {
                logger.warn("Listener stoped!");
            }
            if (dumper.isDone()) {
                logger.warn("Dumper stoped!");
            }
            if (queue.size() > queueBound * 0.99) {
                logger.warn("queue is almost full with: " + queue.size() + " items.");
            }
            if (queue.size() < queueBound * 0.01) {
                logger.warn("queue is almost empty with: " + queue.size() + " items.");
            }
        }
    }

    public static void main(String[] args) throws ParseException, IOException {
        Options options = new Options();
        options.addOption("o", "outdir", true, "output directory");
        options.addOption("k", "keydirectory", true, "api key directory");
        options.addOption("s", "boundsize", true, "bound size for the queque");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        String outputdir = null, keydir = null;
        int queueBound = 10000;
        if (cmd.hasOption("o")) {
            outputdir = cmd.getOptionValue("o");
        }
        if (cmd.hasOption("k")) {
            keydir = cmd.getOptionValue("k");
        }
        if (cmd.hasOption("s")) {
            queueBound = Integer.parseInt(cmd.getOptionValue("s"));
        }
        new SampleHbc().listenDump(keydir, keydir, queueBound);
    }

}
