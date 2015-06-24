package shipit.later;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.SearchHit;

public class IndexDownloader implements Runnable {
	public static final long SCROLL_TIMEOUT = 60000;
	public static final int SCROLL_SIZE = 100;

	private Client client;
	private String index;
	private String outputPath;
	private long time;

	public IndexDownloader(Client client, String index, String outputPath,
			long time) {
		this.client = client;
		this.index = index;
		this.outputPath = outputPath;
		this.time = time;
	}

	@Override
	public void run() {
		try {
			download();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to download index: " + index);
		}
	}

	public void download() throws IOException {
		String fileName = index + "_" + time + ".json";
		String fullFileName = outputPath + "/" + fileName;

		System.out.println("Begin downloading index: " + this.index + " to "
				+ fileName);
		FileOutputStream fos = new FileOutputStream(fullFileName);
		BufferedOutputStream bos = new BufferedOutputStream(fos);
		try {
			SearchResponse scrollResp = client.prepareSearch(index)
					.setSearchType(SearchType.SCAN)
					.setScroll(new TimeValue(SCROLL_TIMEOUT))
					.setQuery(new MatchAllQueryBuilder()).setSize(SCROLL_SIZE)
					.execute().actionGet();
			// Scroll until no hits are returned
			while (true) {
				for (SearchHit hit : scrollResp.getHits().getHits()) {
					bos.write(hit.source());
					bos.write("\n".getBytes());
				}
				System.out.println(Thread.currentThread().getName()
						+ ": flushing... ");
				bos.flush();
				scrollResp = client
						.prepareSearchScroll(scrollResp.getScrollId())
						.setScroll(new TimeValue(SCROLL_TIMEOUT)).execute()
						.actionGet();
				// Break condition: No hits are returned
				if (scrollResp.getHits().getHits().length == 0) {
					break;
				}
			}
		} finally {
			bos.close();
		}
		System.out.println(Thread.currentThread().getName() + ": done");
	}
}
