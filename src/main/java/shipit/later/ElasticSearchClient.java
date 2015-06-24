package shipit.later;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ElasticSearchClient {
	private String host;
	private String cluster;
	private int port;

	public ElasticSearchClient(String host, int port, String cluster) {
		this.host = host;
		this.port = port;
		this.cluster = cluster;
	}

	public Client createClient() {
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", this.cluster).build();
		TransportClient tc = new TransportClient(settings);

		tc.addTransportAddress(new InetSocketTransportAddress(this.host,
				this.port));

		return tc;
	}
}
