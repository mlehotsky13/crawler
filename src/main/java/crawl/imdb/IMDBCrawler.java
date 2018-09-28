package crawl.imdb;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import crawl.Crawler;

public class IMDBCrawler implements Crawler {

	private static final String BASE_URL = "https://www.imdb.com/";
	private static final String SEARCH_TITLE = "search/title";
	private static final String ALL_GENRES = "?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=b9121fa8-b7bb-4a3e-8887-aab822e0b5a7&pf_rd_r=4VPVFKZNBXANDZCFN972&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=moviemeter&ref_=chtmvm_gnr_1&&explore=title_type,genres";

	private static final List<String> WANTED_GENRES = Arrays.asList("Action", "Adventure", "War");

	private static final ObjectMapper om = new ObjectMapper();

	public ArrayNode crawlAndSave() throws IOException {
		ArrayNode titles = om.createArrayNode();

		Document doc = Jsoup.connect(BASE_URL + SEARCH_TITLE + ALL_GENRES).userAgent("Mozilla/5.0").timeout(0).get();
		List<Element> genreItems = doc.selectFirst("h3:contains(Genres)").nextElementSibling().select("a");

		for (Element genreItem : genreItems) {
			if (WANTED_GENRES.contains(genreItem.text())) {
				titles.addAll(parseGenre(BASE_URL + SEARCH_TITLE + genreItem.attr("href")));
			}
		}

		System.out.println(titles.toString());

		return titles;
	}

	private ArrayNode parseGenre(String url) throws IOException {
		ArrayNode genreTitles = om.createArrayNode();

		Document doc = Jsoup.connect(url).userAgent("Mozilla/5.0").timeout(0).get();
		List<Element> titles = doc.select("div[class=lister-item mode-advanced]").stream()
				.map(div -> div.selectFirst("a")).collect(Collectors.toList());

		for (Element title : titles) {
			genreTitles.add(parseTitle(BASE_URL + title.attr("href")));
		}

		// System.out.println(genreTitles.toString());

		return genreTitles;
	}

	private JsonNode parseTitle(String url) throws IOException {
		Document doc = Jsoup.connect(url).userAgent("Mozilla/5.0").timeout(0).get();

		Element heading = doc.selectFirst("h1");
		Element subtext = doc.selectFirst("div[class=subtext]");

		String titleName = getTitleName(doc);
		Optional<Integer> titleYear = getTitleYear(heading);
		Optional<Integer> titleDuration = getTitleDuration(subtext);
		Optional<Double> titleRatingValue = getTitleRatingValue(doc);
		Optional<Integer> titleRatingCount = getTitleRatingCount(doc);
		String titleSummary = doc.selectFirst("div[class=summary_text]").text();
		ArrayNode titleGenres = getTitleGenres(subtext);

		ObjectMapper om = new ObjectMapper();
		ObjectNode on = om.createObjectNode();

		on.put("name", titleName);
		titleYear.ifPresent(ty -> on.put("year", ty));
		titleDuration.ifPresent(td -> on.put("duration", td));
		titleRatingValue.ifPresent(trv -> on.put("ratingValue", trv));
		titleRatingCount.ifPresent(trc -> on.put("ratingCount", trc));
		on.put("summary", titleSummary);
		on.set("genres", titleGenres);

		return on;
	}

	private String getTitleName(Element element) {
		return Optional.ofNullable(element.selectFirst("div[class=originalTitle]")).map(e -> e.ownText())
				.orElse(element.selectFirst("h1").ownText());
	}

	private Optional<Integer> getTitleYear(Element element) {
		return Optional.ofNullable(element.selectFirst("a")).map(e -> Integer.parseInt(e.text()));
	}

	private Optional<Integer> getTitleDuration(Element element) {
		return Optional.ofNullable(element.selectFirst("time"))
				.map(e -> Integer.parseInt(e.attr("datetime").replaceAll("\\D", "")));
	}

	private Optional<Double> getTitleRatingValue(Element element) {
		return Optional.ofNullable(element.selectFirst("span[itemprop=ratingValue]"))
				.map(e -> Double.parseDouble(e.text()));
	}

	private Optional<Integer> getTitleRatingCount(Element element) {
		return Optional.ofNullable(element.selectFirst("span[itemprop=ratingCount]"))
				.map(e -> Integer.parseInt(e.text().replace(",", "")));
	}

	private ArrayNode getTitleGenres(Element element) {
		ArrayNode titleGenres = om.createArrayNode();

		List<Element> aElements = element.select("a");
		for (int i = 0; i < aElements.size() - 1; i++) {
			titleGenres.add(aElements.get(i).text());
		}

		return titleGenres;
	}
}
