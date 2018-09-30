package crawl.imdb;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
    private static final String ALL_GENRES =
            "?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=b9121fa8-b7bb-4a3e-8887-aab822e0b5a7&pf_rd_r=4VPVFKZNBXANDZCFN972&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=moviemeter&ref_=chtmvm_gnr_1&&explore=title_type,genres";

    private static final List<String> WANTED_GENRES = Arrays.asList("Action", "Adventure", "Animation");

    private static final ObjectMapper om = new ObjectMapper();

    public void crawlAndSave() throws IOException, InterruptedException {
        Document doc = Jsoup.connect(BASE_URL + SEARCH_TITLE + ALL_GENRES).userAgent("Mozilla/5.0").timeout(0).get();
        List<Element> genreItems = doc.selectFirst("h3:contains(Genres)").nextElementSibling().select("a");

        ExecutorService es = Executors.newCachedThreadPool();
        for (Element genreItem : genreItems) {
            if (WANTED_GENRES.contains(genreItem.text())) {
                es.submit(() -> new IMDBCrawler().parseGenre(genreItem.text(), genreItem.attr("abs:href"), 10));
            }
        }

        es.shutdown();
        es.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    private int parseGenre(String genre, String url, int limit) throws IOException {
        ArrayNode genreTitles = om.createArrayNode();

        Document doc = Jsoup.connect(url).userAgent("Mozilla/5.0").timeout(0).get();
        genreTitles.addAll(parseGenrePage(doc, limit));

        while (genreTitles.size() < limit) {
            doc = Jsoup.connect(doc.selectFirst("a[class=lister-page-next next-page]").attr("abs:href"))
                    .userAgent("Mozilla/5.0").timeout(0).get();
            genreTitles.addAll(parseGenrePage(doc, limit));
        }

        Path genreFile = Paths.get("src/main/resources/data/titles_" + genre.toLowerCase() + ".json");
        try (BufferedWriter bw = Files.newBufferedWriter(genreFile)) {
            bw.append(genreTitles.toString());
        }
        
        return genreTitles.size();
    }

    private ArrayNode parseGenrePage(Document doc, int limit) throws IOException {
        ArrayNode genreTitles = om.createArrayNode();

        List<Element> titles = doc.select("div[class=lister-item mode-advanced]").stream()
                .map(div -> div.selectFirst("a")).collect(Collectors.toList());

        for (int i = 0; i < titles.size() && i < limit; i++) {
            genreTitles.add(parseTitle(titles.get(i).attr("abs:href")));
        }

        return genreTitles;
    }

    private JsonNode parseTitle(String url) throws IOException {
        Document doc = Jsoup.connect(url).userAgent("Mozilla/5.0").timeout(0).get();

        Element script = doc.selectFirst("script[type=application/ld+json]");
        JsonNode scriptNode = om.readTree(script.dataNodes().get(0).toString());

        ObjectNode on = om.createObjectNode();
        getTitleName(scriptNode).ifPresent(v -> on.set("name", v));
        getTitleType(scriptNode).ifPresent(v -> on.set("type", v));
        getTitlePublishDate(scriptNode).ifPresent(v -> on.set("datePublished", v));
        getTitleDuration(scriptNode).ifPresent(td -> on.set("duration", td));
        getTitleRating(scriptNode).ifPresent(v -> on.set("rating", v));
        getTitleDirector(scriptNode).ifPresent(v -> on.set("director", v));
        getTitleActors(scriptNode).ifPresent(v -> on.set("actors", v));
        getTitleGenres(scriptNode).ifPresent(v -> on.set("genres", v));
        getTitleKeywords(scriptNode).ifPresent(v -> on.set("keywords", v));
        getTitleDescription(doc).ifPresent(v -> on.put("description", v));

        return on;
    }

    private Optional<JsonNode> getTitleName(JsonNode node) {
        return Optional.ofNullable(node.get("name"));
    }

    private Optional<JsonNode> getTitleType(JsonNode node) {
        return Optional.ofNullable(node.get("@type"));
    }

    private Optional<JsonNode> getTitlePublishDate(JsonNode node) {
        return Optional.ofNullable(node.get("datePublished"));
    }

    private Optional<JsonNode> getTitleDuration(JsonNode node) {
        return Optional.ofNullable(node.get("duration"));
    }

    private Optional<JsonNode> getTitleRating(JsonNode node) {
        return Optional.ofNullable(node.get("aggregateRating"));
    }

    private Optional<JsonNode> getTitleDirector(JsonNode node) {
        return Optional.ofNullable(node.get("director"));
    }

    private Optional<JsonNode> getTitleActors(JsonNode node) {
        return Optional.ofNullable(node.get("actor"));
    }

    private Optional<JsonNode> getTitleGenres(JsonNode node) {
        return Optional.ofNullable(node.get("genres"));
    }

    private Optional<JsonNode> getTitleKeywords(JsonNode node) {
        return Optional.ofNullable(node.get("keywords"));
    }

    private Optional<String> getTitleDescription(Document doc) {
        return Optional.ofNullable(doc.selectFirst("div[class=summary_text]")).map(v -> v.text());
    }
}
