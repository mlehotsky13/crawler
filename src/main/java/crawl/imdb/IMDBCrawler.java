package crawl.imdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.fasterxml.jackson.databind.JsonNode;

import crawl.Crawler;

public class IMDBCrawler implements Crawler {

	private static final String BASE_URL = "https://www.imdb.com/";
	private static final String SEARCH_TITLE = "search/title";
	private static final String ALL_GENRES = "?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=b9121fa8-b7bb-4a3e-8887-aab822e0b5a7&pf_rd_r=4VPVFKZNBXANDZCFN972&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=moviemeter&ref_=chtmvm_gnr_1&&explore=title_type,genres";

	private static final List<String> NOT_WANTED_GENRES = Arrays.asList();

	public List<JsonNode> crawlAndSave() throws IOException {
		List<JsonNode> titles = new ArrayList<>();

		Document doc = Jsoup.connect(BASE_URL + SEARCH_TITLE + ALL_GENRES).userAgent("Mozilla/5.0").timeout(0).get();
		List<Element> genreItems = doc.selectFirst("h3:contains(Genres)").nextElementSibling().select("a");

		for (Element genreItem : genreItems) {
			if (!NOT_WANTED_GENRES.contains(genreItem.text())) {
				titles.addAll(parseGenre(BASE_URL + SEARCH_TITLE + genreItem.attr("href")));
			}
		}

		return titles;
	}

	private List<JsonNode> parseGenre(String url) throws IOException {
		List<JsonNode> genreTitles = new ArrayList<>();

		Document doc = Jsoup.connect(url).userAgent("Mozilla/5.0").timeout(0).get();
		List<Element> titles = doc.select("div[class=lister-item mode-advanced]").stream()
				.map(div -> div.selectFirst("a")).collect(Collectors.toList());

		for (Element title : titles) {
			genreTitles.add(parseTitle(BASE_URL + title.attr("href")));
		}

		return genreTitles;
	}

	private JsonNode parseTitle(String url) {
		return null;
	}
}
