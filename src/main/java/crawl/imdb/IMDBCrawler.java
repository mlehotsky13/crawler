package crawl.imdb;

import java.io.IOException;
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

import crawl.AbstractCrawler;

public class IMDBCrawler extends AbstractCrawler {

    private static final String BASE_URL = "https://www.imdb.com/";
    private static final String SEARCH_TITLE = "search/title";
    private static final String ALL_GENRES =
            "?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=b9121fa8-b7bb-4a3e-8887-aab822e0b5a7&pf_rd_r=4VPVFKZNBXANDZCFN972&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=moviemeter&ref_=chtmvm_gnr_1&&explore=title_type,genres";

    private static final List<String> NOT_WANTED_GENRES = Arrays.asList("Action");

    private static final ObjectMapper om = new ObjectMapper();

    public void crawlAndSave() throws IOException, InterruptedException {
        Document doc = Jsoup.connect(BASE_URL + SEARCH_TITLE + ALL_GENRES).userAgent("Mozilla/5.0").timeout(0).get();
        List<Element> genreItems = doc.selectFirst("h3:contains(Genres)").nextElementSibling().select("a");

        ExecutorService es = Executors.newCachedThreadPool();
        for (Element genreItem : genreItems) {
            if (NOT_WANTED_GENRES.contains(genreItem.text())) {
                es.submit(() -> parseGenre(genreItem.text(), genreItem.attr("abs:href"), 10));
            }
        }

        es.shutdown();
        es.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    private int parseGenre(String genre, String url, int limit) throws IOException {
        ArrayNode genreTitles = om.createArrayNode();

        System.out.println("Parsing genre " + genre + " ...");

        try {
            Document doc = Jsoup.connect(url).userAgent("Mozilla/5.0").timeout(0).get();
            genreTitles.addAll(parseGenrePage(doc, limit));

            while ((url = getNextPage(doc)) != null && genreTitles.size() < limit) {
                doc = Jsoup.connect(url).userAgent("Mozilla/5.0").timeout(0).get();
                genreTitles.addAll(parseGenrePage(doc, limit));
            }
        } finally {
            // Path genreFile = Paths.get("src/main/resources/data/titles_" + genre.toLowerCase() + ".json");
            Path genreFile = Paths.get("/home/miroslav/Desktop/" + genre.toLowerCase() + ".json");
            writeToFile(genreFile, genreTitles.toString());
        }

        return genreTitles.size();
    }

    private ArrayNode parseGenrePage(Document doc, int limit) throws IOException {
        ArrayNode genreTitles = om.createArrayNode();

        List<Element> titles = doc.select("div[class=lister-item mode-advanced]").stream().map(div -> div.selectFirst("a"))
                .collect(Collectors.toList());

        for (int i = 0; i < titles.size() && i < limit; i++) {
            genreTitles.add(parseTitle(titles.get(i).attr("abs:href")));
        }

        System.out.println("Parsed genre page with " + genreTitles.size() + " titles.");

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
        getTitleGenres(scriptNode).ifPresent(v -> on.set("genres", v));
        getTitleKeywords(scriptNode).ifPresent(v -> on.set("keywords", v));
        getTitleDescription(doc).ifPresent(v -> on.put("description", v));

        Optional<Document> castDoc = getCastDoc(doc);
        on.set("cast", getCast(castDoc));
        on.set("writers", getWriters(castDoc));
        on.set("directors", getDirectors(castDoc));
        on.set("producers", getProducers(castDoc));

        // try {
        // Thread.currentThread().sleep(1000);
        // } catch (Exception e) {
        // e.printStackTrace();
        // }

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

    private Optional<JsonNode> getTitleGenres(JsonNode node) {
        return Optional.ofNullable(node.get("genres"));
    }

    private Optional<JsonNode> getTitleKeywords(JsonNode node) {
        return Optional.ofNullable(node.get("keywords"));
    }

    private Optional<String> getTitleDescription(Document doc) throws IOException {
        Element descDiv = doc.selectFirst("div[class=summary_text]");

        if (descDiv != null && descDiv.selectFirst("a") != null) {
            return getFullTitleDescription(descDiv);
        }

        return Optional.ofNullable(descDiv).map(v -> v.text());
    }

    private Optional<String> getFullTitleDescription(Element element) throws IOException {
        Optional<String> fullSummaryURL =
                Optional.ofNullable(element.selectFirst("a:contains(See full summary)")).map(v -> v.attr("abs:href"));

        if (fullSummaryURL.isPresent()) {
            Document summaryDoc = Jsoup.connect(fullSummaryURL.get()).userAgent("Mozilla/5.0").timeout(0).get();
            return Optional.of(summaryDoc.selectFirst("h4[id=summaries]").nextElementSibling().selectFirst("p").text());
        }

        return Optional.empty();
    }

    private String getNextPage(Document doc) {
        return Optional.ofNullable(doc.selectFirst("a[class=lister-page-next next-page]")).map(v -> v.attr("abs:href"))
                .orElse(null);
    }

    private JsonNode getCast(Optional<Document> castDoc) throws IOException {
        ArrayNode an = om.createArrayNode();

        if (castDoc.isPresent()) {
            an = parseFullCast(castDoc.get());
        }

        return an;
    }

    private JsonNode getWriters(Optional<Document> castDoc) throws IOException {
        ArrayNode an = om.createArrayNode();

        if (castDoc.isPresent()) {
            an = parseWriters(castDoc.get());
        }

        return an;
    }

    private JsonNode getDirectors(Optional<Document> castDoc) throws IOException {
        ArrayNode an = om.createArrayNode();

        if (castDoc.isPresent()) {
            an = parseDirectors(castDoc.get());
        }

        return an;
    }

    private JsonNode getProducers(Optional<Document> castDoc) throws IOException {
        ArrayNode an = om.createArrayNode();

        if (castDoc.isPresent()) {
            an = parseProducers(castDoc.get());
        }

        return an;
    }

    private Optional<Document> getCastDoc(Document doc) {
        return getFullCastURL(doc).map(v -> {
            Document castDoc = null;
            try {
                castDoc = Jsoup.connect(v).userAgent("Mozilla/5.0").maxBodySize(0).timeout(0).get();
            } catch (Exception e) {
                e.printStackTrace();
            }

            return castDoc;
        });
    }

    private Optional<String> getFullCastURL(Document doc) {
        return doc.select("div[class=see-more]").stream().filter(v -> v.selectFirst("a:contains(See full cast)") != null).limit(1)
                .map(v -> v.selectFirst("a").attr("abs:href")).findFirst();
    }

    private ArrayNode parseFullCast(Document doc) {
        ArrayNode an = om.createArrayNode();

        Optional<Element> table = Optional.ofNullable(doc.selectFirst("table[class=cast_list]"));
        if (table.isPresent()) {
            List<Element> validCastRows = getValidCastRows(table.get());
            for (Element row : validCastRows) {
                ObjectNode on = om.createObjectNode();
                on.put("name", row.select("td").get(1).text());
                on.put("character", row.selectFirst("td[class=character]").text());

                an.add(on);
            }
        }

        return an;
    }

    private ArrayNode parseWriters(Document doc) {
        ArrayNode an = om.createArrayNode();

        Optional<Element> writersHeading = Optional.ofNullable(doc.selectFirst("h4:contains(Writing Credits)"));
        Optional<Element> table = writersHeading.map(v -> v.nextElementSibling());
        if (table.isPresent()) {
            List<Element> validWritersRows = getValidWritersRows(table.get());
            for (Element row : validWritersRows) {
                an.add(row.selectFirst("td").text());
            }
        }

        return an;
    }

    private ArrayNode parseDirectors(Document doc) {
        ArrayNode an = om.createArrayNode();

        Optional<Element> directorsHeading = Optional.ofNullable(doc.selectFirst("h4:contains(Directed by)"));
        Optional<Element> table = directorsHeading.map(v -> v.nextElementSibling());
        if (table.isPresent()) {
            List<Element> validDirectorsRows = getValidDirectorsRows(table.get());
            for (Element row : validDirectorsRows) {
                an.add(row.selectFirst("td").text());
            }
        }

        return an;
    }

    private ArrayNode parseProducers(Document doc) {
        ArrayNode an = om.createArrayNode();

        Optional<Element> producersHeading = Optional.ofNullable(doc.selectFirst("h4:contains(Produced by)"));
        Optional<Element> table = producersHeading.map(v -> v.nextElementSibling());
        if (table.isPresent()) {
            List<Element> validProducersRows = getValidProducersRows(table.get());
            for (Element row : validProducersRows) {
                an.add(row.selectFirst("td").text());
            }
        }

        return an;
    }

    private List<Element> getValidCastRows(Element element) {
        return element.select("tr").stream().filter(v -> v.selectFirst("td[class=character]") != null)
                .collect(Collectors.toList());
    }

    private List<Element> getValidWritersRows(Element element) {
        return element.select("tr").stream().filter(v -> v.selectFirst("td[class=name]") != null).collect(Collectors.toList());
    }

    private List<Element> getValidDirectorsRows(Element element) {
        return element.select("tr").stream().filter(v -> v.selectFirst("td[class=name]") != null).collect(Collectors.toList());
    }

    private List<Element> getValidProducersRows(Element element) {
        return element.select("tr").stream().filter(v -> v.selectFirst("td[class=name]") != null).collect(Collectors.toList());
    }
}
