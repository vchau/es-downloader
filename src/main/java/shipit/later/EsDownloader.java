package shipit.later;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectObjectCursor;

public class EsDownloader {
	private Client client;

	public EsDownloader(Client client) {
		this.client = client;
	}

	private List<String> findMatchingIndices(String regex) {
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

	public void download(String indexRegex, String outputPath) throws Exception {
		List<String> indices = findMatchingIndices(indexRegex);
		long now = System.currentTimeMillis();
		List<Thread> threads = new ArrayList<Thread>(indices.size());
		for (String index : indices) {
			IndexDownloader id = new IndexDownloader(this.client, index,
					outputPath, now);
			Thread t = new Thread(id);
			t.setName(index);
			threads.add(t);
			t.start();
		}

		for (Thread t : threads) {
			t.join();
		}
		System.out.println("All done!");
	}
}
