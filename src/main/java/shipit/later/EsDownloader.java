package shipit.later;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.SearchHit;

public class EsDownloader {
	private String host;
	private String cluster;
	private int port;
	private Client client;

	public EsDownloader(String host, int port, String cluster) {
		this.host = host;
		this.port = port;
		this.cluster = cluster;
	}

	private void setup() {
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", this.cluster).build();
		TransportClient tc = new TransportClient(settings);

		tc.addTransportAddress(new InetSocketTransportAddress(this.host,
				this.port));

		client = tc;
	}

	private List<String> getAllIndices(String regex) {
		List<String> result = new ArrayList<String>();
		ImmutableOpenMap<String, IndexMetaData> indices = client.admin()
				.cluster().prepareState().execute().actionGet().getState()
				.getMetaData().indices();

		Iterator<ObjectObjectCursor<String, IndexMetaData>> iter = indices
				.iterator();

		while (iter.hasNext()) {
			ObjectObjectCursor<String, IndexMetaData> i = iter.next();
			if (i.key.matches(regex)) {
				result.add(i.key);
			}
		}
		return result;
	}

	public void downloadAll(String indexRegex, String outputPath)
			throws Exception {
		setup();

		List<String> indices = getAllIndices(indexRegex);
		String now = String.valueOf(System.currentTimeMillis());
		for (String index : indices) {
			String fileName = outputPath + "/" + index + "_" + now + ".json";

			System.out.println("Writing: " + fileName);
			FileOutputStream fos = new FileOutputStream(fileName);
			BufferedOutputStream bos = new BufferedOutputStream(fos);
			SearchResponse scrollResp = client.prepareSearch(index)
					.setSearchType(SearchType.SCAN)
					.setScroll(new TimeValue(60000))
					.setQuery(new MatchAllQueryBuilder()).setSize(100)
					.execute().actionGet();
			// Scroll until no hits are returned
			while (true) {
				for (SearchHit hit : scrollResp.getHits().getHits()) {
					bos.write(hit.source());
					bos.write("\n".getBytes());
				}
				System.out.println("Flushing...");
				bos.flush();
				scrollResp = client
						.prepareSearchScroll(scrollResp.getScrollId())
						.setScroll(new TimeValue(600000)).execute().actionGet();
				// Break condition: No hits are returned
				if (scrollResp.getHits().getHits().length == 0) {
					break;
				}
			}

			bos.close();
		}
	}
}
