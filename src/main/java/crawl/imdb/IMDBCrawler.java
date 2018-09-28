package crawl.imdb;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import crawl.Crawler;

public class IMDBCrawler implements Crawler {

	private static final String BASE_URL = "https://www.imdb.com/search/title";
	private static final String ALL_GENRES = "?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=b9121fa8-b7bb-4a3e-8887-aab822e0b5a7&pf_rd_r=4VPVFKZNBXANDZCFN972&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=moviemeter&ref_=chtmvm_gnr_1&&explore=title_type,genres";

	private static final List<String> NOT_WANTED_GENRES = Arrays.asList();

	public void crawlAndSave() throws IOException {
		Map<String, String> genres = getGenres();

		for (Entry<String, String> entry : genres.entrySet()) {
			if (!NOT_WANTED_GENRES.contains(entry.getKey())) {
				System.out.println("Parsing genre " + entry.getKey() + " from url: " + entry.getValue());
			}
		}
	}

	private Map<String, String> getGenres() throws IOException {
		Document doc = Jsoup.connect(BASE_URL + ALL_GENRES).userAgent("Mozilla/5.0").timeout(0).get();
		Element genresHeading = doc.select("h3:contains(Genres)").first();
		Element genresTable = genresHeading.nextElementSibling();

		return parseGenresTable(genresTable);
	}

	private Map<String, String> parseGenresTable(Element genresTable) {
		Map<String, String> genres = new HashMap<>();
		List<Element> genreItems = genresTable.select("a");

		for (Element genreItem : genreItems) {
			genres.put(genreItem.text(), genreItem.attr("href"));
		}

		return genres;
	}
}
