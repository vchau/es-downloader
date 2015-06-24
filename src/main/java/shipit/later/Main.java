package shipit.later;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;

public class Main {
	public static final String ARG_INDEX_REGEX = "i";
	public static final String ARG_OUTPUT_PATH = "o";
	public static final String ARG_ES_HOST = "e";
	public static final String ARG_ES_PORT = "p";
	public static final String ARG_ES_CLUSTER = "c";

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(ARG_ES_HOST, true,
				"(Required) Elasticsearch host name.");
		options.addOption(ARG_ES_PORT, true,
				"(Optional) Elasticsearch port. Default: 9200");
		options.addOption(ARG_ES_CLUSTER, true,
				"(Optional) Elasticsearch cluster name. Default: elasticsearch");
		options.addOption(ARG_INDEX_REGEX, true,
				"(Optional) Regex of index names to download. Default: .*");
		options.addOption(ARG_OUTPUT_PATH, true,
				"(Optional) Output directory path. Default: /tmp/elastic");

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(options, args);
		String host = cmd.getOptionValue(ARG_ES_HOST);
		int port = Integer.valueOf(cmd.getOptionValue(ARG_ES_PORT, "9200"));
		String cluster = cmd.getOptionValue(ARG_ES_CLUSTER, "elasticsearch");
		String regex = cmd.getOptionValue(ARG_INDEX_REGEX, ".*");
		String outputPath = cmd.getOptionValue(ARG_INDEX_REGEX, "/tmp/elastic");

		EsDownloader downloader = new EsDownloader(host, port, cluster);

		try {
			downloader.downloadAll(regex, outputPath);
		} catch (Exception e) {
			throw new RuntimeException("Failed.");
		}
	}

}
