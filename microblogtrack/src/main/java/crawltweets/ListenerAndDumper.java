package crawltweets;

import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.log4j.Logger;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class ListenerAndDumper {
	//TODO: process ondelete, filter the tweets
	
	
	final Logger logger = Logger.getLogger(ListenerAndDumper.class);
	
	// api-key and the time stamp recording the latest usage;
	// idealy we want to reuse the api-key after it has been freely for long
	// enough time
	private TObjectLongHashMap<String> apikeyTimestamp = new TObjectLongHashMap<String>();
	// recorder for multiple api-keys, for the sake of robustness
	private HashMap<String, ConfigurationBuilder> apikayConfBuilder = new HashMap<String, ConfigurationBuilder>();
	// current established connection
	private TwitterStream currenttwitter;
	// current api-key name, corresponding to current established connection
	private String currentKey = "";

	private String keydirectory = "";
	// dumper
	protected Archiver achiver;

	private class Archiver {

		private String zipdirectory = null;

		private String zipfilename = "";
		// virtual zip file location
		private URI zip_disk = null;

		private final int buffersize = 10000;

		private final TLongObjectHashMap<byte[]> tweetbuffer = new TLongObjectHashMap<byte[]>();

		private Map<String, String> zip_properties;

		private DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHH");

		public Archiver(String zipdirectory) {
			this.zipdirectory = zipdirectory;
			zip_properties = new HashMap<String, String>();
			this.zip_properties.put("create", "true");
		}

		public void createZip() {
			String zipfulluri = new File(this.zipdirectory, this.zipfilename)
					.toString();
			zip_disk = URI.create("jar:file:" + zipfulluri);
		}

		private String zipnameInDate() {
			Date date = new Date();
			return dateFormat.format(date) + ".zip";
		}

		private void addToArchive(long tweetid, String rawJson)
				throws IOException, ArchiveException {
			Path root, filename;
			tweetbuffer.put(tweetid, rawJson.getBytes());
			if (tweetbuffer.size() >= buffersize) {
				// create new zip file periodically
				String currentZipDateFilename = zipnameInDate();
				if (!zipfilename.equals(currentZipDateFilename)) {
					zipfilename = currentZipDateFilename;
					createZip();
				}
				try (FileSystem zipfs = FileSystems.newFileSystem(zip_disk,
						zip_properties)) {
					for (long tid : tweetbuffer.keys()) {
						root = zipfs.getPath("/");
						filename = zipfs.getPath(root.toString(),
								String.valueOf(tid) + ".json");
						if (!Files.exists(filename)){
							Files.write(filename, tweetbuffer.get(tid),StandardOpenOption.CREATE,
						         StandardOpenOption.TRUNCATE_EXISTING);
						}else{
							logger.error(tweetid + " already exists in " + zipfilename);
						}
					}
				}
				tweetbuffer.clear();
			}
		}
	}

	private class MyStatusListener implements StatusListener {
		private ListenerAndDumper lad;
		private int minutecount = 0;
		private Timer timer = new Timer();

		private class Printcount extends TimerTask {
			private DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
			public void run() {
				Date date = new Date();
				System.out.println(dateFormat.format(date) + " " + minutecount);
				minutecount = 0;
			}
		}

		public MyStatusListener(ListenerAndDumper lad) {
			this.lad = lad;
			timer.schedule(new Printcount(), 0, 600000);
		}

		@Override
		public void onStatus(Status status) {
			minutecount++;
			long tweetid = status.getId();
			String rawJSON = TwitterObjectFactory.getRawJSON(status);
			try {
				lad.achiver.addToArchive(tweetid, rawJSON);
			} catch (IOException | ArchiveException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
//			 System.err.println("Deletion notice:"
//			 + statusDeletionNotice.getStatusId());
		}

		@Override
		public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
			logger.error("numberOfLimitedStatuses: " + numberOfLimitedStatuses);
		}

		@Override
		public void onException(Exception ex) {
			logger.error(ex);
			// reconnect when error
			try {
				lad.restart();
			} catch (InterruptedException | FileNotFoundException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void onScrubGeo(long userId, long upToStatusId) {
			logger.warn("Got scrub_geo event userId:" + userId
					+ " upToStatusId:" + upToStatusId);
		}

		@Override
		public void onStallWarning(StallWarning arg0) {
			logger.warn(arg0.getMessage() + ", PercentFull:"
					+ arg0.getPercentFull());
		}
	}

	/**
	 * main entrance: listener to dump all tweets received
	 * 
	 * @param keydirectory
	 * @param zipdirectory
	 * @param expid
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public ListenerAndDumper(String keydirectory, String zipdirectory)
			throws IOException {
		// read-in multiple api-keys, one for connection, others for spare
		readinAPIConfBuilder(keydirectory);
		this.keydirectory = keydirectory;
		// dump the tweets into zipdirectory, one hour/day one zip file
		achiver = new Archiver(zipdirectory);

	}

	/**
	 * establish connection and start listening
	 * 
	 * @throws FileNotFoundException
	 */
	public void restart() throws InterruptedException, FileNotFoundException {
		// a listener
		StatusListener statuslistener = new MyStatusListener(this);
		// establish the connection with twitter api; also used when running
		// into errors
		updateListener(statuslistener, keydirectory);
	}

	/**
	 * read in multiple api-keys and store in apikayConfBuilder, associating
	 * with corresponding time stamp indicating the latest usage
	 * 
	 * @param keydirectory
	 * @param expid
	 * @throws IOException
	 */
	private void readinAPIConfBuilder(String keydirectory) throws IOException {
		BufferedReader br;
		int keynum = 0;
		String oauthConsumerKey = null, oauthConsumerSecret = null, oauthAccessToken = null, oauthAccessTokenSecret = null;
		// key-timestamp records when the key being used most latest
		br = new BufferedReader(new FileReader(new File(keydirectory,
				"key-timestamp")));
		while (br.ready()) {
			String line = br.readLine();
			String[] cols = line.split(" ");
			if (cols.length == 2) {
				apikeyTimestamp.put(cols[0], Long.parseLong(cols[1]));
			}
		}
		br.close();
		// read in multiple keys for backup
		for (String keyfile : new File(keydirectory).list()) {
			if (keyfile.equals("key-timestamp"))
				continue;
			br = new BufferedReader(new FileReader(new File(keydirectory,
					keyfile)));
			while (br.ready()) {
				String line = br.readLine();
				String[] cols = line.split("=");
				if (cols[0].equals("oauth.consumerKey")) {
					oauthConsumerKey = cols[1];
				} else if (cols[0].equals("oauth.consumerSecret")) {
					oauthConsumerSecret = cols[1];
				} else if (cols[0].equals("oauth.accessToken")) {
					oauthAccessToken = cols[1];
				} else if (cols[0].equals("oauth.accessTokenSecret")) {
					oauthAccessTokenSecret = cols[1];
				}
			}
			br.close();
			apikayConfBuilder.put(keyfile, new ConfigurationBuilder());
			apikayConfBuilder.get(keyfile).setJSONStoreEnabled(true)
					.setOAuthConsumerKey(oauthConsumerKey)
					.setOAuthConsumerSecret(oauthConsumerSecret)
					.setOAuthAccessToken(oauthAccessToken)
					.setOAuthAccessTokenSecret(oauthAccessTokenSecret);
		}
	}

	/**
	 * pick up the api-key, being spared for longest time and establish the
	 * standing connection with twitter api
	 * 
	 * @param statuslistener
	 * @throws InterruptedException
	 * @throws FileNotFoundException
	 */
	private void updateListener(StatusListener statuslistener,
			String keydirectory) throws InterruptedException,
			FileNotFoundException {
		ConfigurationBuilder cb;
		long currentTime = System.currentTimeMillis();
		long minimumTime = currentTime;
		if (apikeyTimestamp.containsKey(currentKey)) {
			apikeyTimestamp.adjustValue(currentKey, currentTime);
		}
		if (apikayConfBuilder.size() > 1) {
			long[] milsecond = apikeyTimestamp.values();
			Arrays.sort(milsecond);
			minimumTime = milsecond[0];
		}
		for (String apikey : apikayConfBuilder.keySet()) {
			if (apikeyTimestamp.get(apikey) <= minimumTime) {
				cb = apikayConfBuilder.get(apikey);
				currenttwitter = new TwitterStreamFactory(cb.build())
						.getInstance();
				currenttwitter.addListener(statuslistener);
				currenttwitter.sample();
				currentKey = apikey;
				// api-key should be spared for more than 15 min (the length of
				// time window)
				if ((currentTime - minimumTime) <= 15 * 1000) {
					Thread.sleep(currentTime - minimumTime);
				}
				break;
			}
		}
		logger.info(currentKey + " is being used to connect twiter API.");
		// update and rewrite the file records the key and the time stamp
		currentTime = System.currentTimeMillis();
		PrintStream ps = new PrintStream(
				new File(keydirectory, "key-timestamp"));
		for (String apikey : apikeyTimestamp.keys(new String[0])) {
			if (apikey.equals(currentKey)) {
				ps.println(currentKey + " " + String.valueOf(currentTime));
				continue;
			}
			ps.println(apikey + " "
					+ String.valueOf(apikeyTimestamp.get(apikey)));
		}
		ps.close();
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ParseException {
		Options options = new Options();
		options.addOption("o", "outdir", true, "output directory");
		options.addOption("k", "keydirectory", true, "api key directory");
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(options, args);
		String zipdirectory = null, keydirectory = null;

		if (cmd.hasOption("o")) {
			zipdirectory = cmd.getOptionValue("o");
		}
		if (cmd.hasOption("k")) {
			keydirectory = cmd.getOptionValue("k");
		}

		ListenerAndDumper lad = new ListenerAndDumper(keydirectory,
				zipdirectory);
		lad.restart();
	}

}
