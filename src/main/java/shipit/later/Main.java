package shipit.later;

import java.io.File;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class Main {
	public static final String ARG_INDEX_REGEX = "i";
	public static final String ARG_OUTPUT_PATH = "o";
	public static final String ARG_ES_HOST = "h";
	public static final String ARG_ES_PORT = "p";
	public static final String ARG_ES_CLUSTER = "c";

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(ARG_ES_HOST, true,
				"(Required) Elasticsearch host name.");
		options.addOption(ARG_ES_PORT, true,
				"(Optional) Elasticsearch port. Default: 9300");
		options.addOption(ARG_ES_CLUSTER, true,
				"(Optional) Elasticsearch cluster name. Default: elasticsearch");
		options.addOption(ARG_INDEX_REGEX, true,
				"(Optional) Regex of index names to download. Default: .*");
		options.addOption(ARG_OUTPUT_PATH, true,
				"(Optional) Output directory path. Default: /tmp/elastic");

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(options, args);
		String host = cmd.getOptionValue(ARG_ES_HOST);
		int port = Integer.valueOf(cmd.getOptionValue(ARG_ES_PORT, "9300"));
		String cluster = cmd.getOptionValue(ARG_ES_CLUSTER, "elasticsearch");
		String regex = cmd.getOptionValue(ARG_INDEX_REGEX, ".*");
		String outputPath = cmd.getOptionValue(ARG_OUTPUT_PATH, "/tmp/elastic");

		if (host == null || host.isEmpty()) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("es-downloader -[OPTION] [value]", options);
			return;
		}

		ElasticSearchClient es = new ElasticSearchClient(host, port, cluster);
		EsDownloader downloader = new EsDownloader(es.createClient());

		File outputDir = new File(outputPath);
		if (!outputDir.exists()) {
			outputDir.mkdirs();
			System.out.println("Created output path: " + outputPath);
		}

		downloader.download(regex, outputPath);
	}
}
