package crawl.imdb;

import java.io.IOException;
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
            "?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=b9121fa8-b7bb-4a3e-8887-aab822e0b5a7&pf_rd_r=4VPVFKZNBXANDZCFN972&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=moviemeter&explore=title_type,genres&page=3&ref_=adv_nxt";

    private static final List<String> NOT_WANTED_GENRES =
            Arrays.asList("Film-Noir", "Talk-Show", "News", "Reality-TV", "Musical", "Adult", "Short", "Game-Show");

    private static final List<String> WANTED_GENRES = Arrays.asList("Action");

    private static final ObjectMapper om = new ObjectMapper();

    public void crawlAndSave() throws IOException, InterruptedException {
        Document doc =
                Jsoup.connect(BASE_URL + SEARCH_TITLE + ALL_GENRES).userAgent("Mozilla/5.0").maxBodySize(0).timeout(0).get();
        List<Element> genreItems = doc.selectFirst("h3:contains(Genres)").nextElementSibling().select("a");

        ExecutorService es = Executors.newFixedThreadPool(5);
        for (Element genreItem : genreItems) {
            if (!NOT_WANTED_GENRES.contains(genreItem.text())) {
                try {
                    Document genreDoc =
                            Jsoup.connect(genreItem.attr("abs:href")).userAgent("Mozilla/5.0").maxBodySize(0).timeout(0).get();
                    es.submit(() -> new IMDBCrawler().parseGenre(genreItem.text(), genreDoc, 500));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        es.shutdown();
        es.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    private int parseGenre(String genre, Document doc, int limit) throws IOException {
        ArrayNode genreTitles = om.createArrayNode();
        int page = 1;

        System.out.println("Parsing genre " + genre + " ...");

        genreTitles.addAll(parseGenrePage(doc, limit));

        String nextPageURL = getNextPage(doc);
        while ((nextPageURL = getNextPage(doc)) != null && genreTitles.size() < limit && page <= 10) {
            try {
                doc = Jsoup.connect(nextPageURL).userAgent("Mozilla/5.0").maxBodySize(0).timeout(0).get();
                genreTitles.addAll(parseGenrePage(doc, limit));
            } catch (IOException e) {
                e.printStackTrace();
            }

            page++;
        }

        // Path genreFile = Paths.get("src/main/resources/data/titles_" + genre.toLowerCase() + ".json");
        // writeToFile(genreFile, genreTitles.toString());

        return genreTitles.size();
    }

    private ArrayNode parseGenrePage(Document doc, int limit) throws IOException {
        ArrayNode genreTitles = om.createArrayNode();

        List<Element> titles = doc.select("div[class=lister-item mode-advanced]").stream().map(div -> div.selectFirst("a"))
                .collect(Collectors.toList());

        for (int i = 0; i < titles.size() && i < limit; i++) {
            try {
                doc = Jsoup.connect(titles.get(i).attr("abs:href")).userAgent("Mozilla/5.0").maxBodySize(0).timeout(0).get();
                genreTitles.add(parseTitle(doc));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Parsed genre page with " + genreTitles.size() + " titles.");

        return genreTitles;
    }

    private JsonNode parseTitle(Document doc) throws IOException {
        ObjectNode on = om.createObjectNode();

        try {
            Element script = doc.selectFirst("script[type=application/ld+json]");
            JsonNode scriptNode = om.readTree(script.dataNodes().get(0).toString());

            Optional<Document> castDoc = getCastDoc(doc);
            Optional<Document> summaryDoc = getFullTitleDescriptionDoc(doc);

            String titleName = getTitleName(scriptNode).map(v -> v.textValue()).orElse("Unknown_" + System.currentTimeMillis());
            titleName = titleName.replaceAll("/", "").replaceAll(" ", "_");

            writeToFile(Paths.get("src/main/resources/data/imdb/pages/", titleName + ".html"), doc.toString());

            if (castDoc.isPresent())
                writeToFile(Paths.get("src/main/resources/data/imdb/pages/", titleName + "_cast.html"), castDoc.get().toString());
            if (summaryDoc.isPresent())
                writeToFile(Paths.get("src/main/resources/data/imdb/pages/", titleName + "_summary.html"),
                        summaryDoc.get().toString());


            // getTitleName(scriptNode).ifPresent(v -> on.set("name", v));
            // getTitleType(scriptNode).ifPresent(v -> on.set("type", v));
            // getTitlePublishDate(scriptNode).ifPresent(v -> on.set("datePublished", v));
            // getTitleDuration(scriptNode).ifPresent(td -> on.set("duration", td));
            // getTitleRating(scriptNode).ifPresent(v -> on.set("rating", v));
            // getTitleGenres(scriptNode).ifPresent(v -> on.set("genres", v));
            // getTitleKeywords(scriptNode).ifPresent(v -> on.set("keywords", v));
            // getTitleDescription(doc).ifPresent(v -> on.put("description", v));

            // Optional<Document> castDoc = getCastDoc(doc);
            // on.set("cast", getCast(castDoc));
            // on.set("writers", getWriters(castDoc));
            // on.set("directors", getDirectors(castDoc));
            // on.set("producers", getProducers(castDoc));
        } catch (IOException e) {
            e.printStackTrace();
        }

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
        return Optional.ofNullable(node.get("genre"));
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

    private Optional<Document> getFullTitleDescriptionDoc(Element element) throws IOException {
        Element summaryDiv = element.selectFirst("div[class=summary_text]");

        if (summaryDiv != null) {
            Optional<String> fullSummaryURL =
                    Optional.ofNullable(summaryDiv.selectFirst("a:contains(See full summary)")).map(v -> v.attr("abs:href"));

            if (fullSummaryURL.isPresent()) {
                return Optional.of(Jsoup.connect(fullSummaryURL.get()).userAgent("Mozilla/5.0").maxBodySize(0).timeout(0).get());
            }

        }

        return Optional.empty();
    }

    private Optional<String> getFullTitleDescription(Element element) throws IOException {
        Optional<String> fullSummaryURL =
                Optional.ofNullable(element.selectFirst("a:contains(See full summary)")).map(v -> v.attr("abs:href"));

        if (fullSummaryURL.isPresent()) {
            Document summaryDoc = Jsoup.connect(fullSummaryURL.get()).userAgent("Mozilla/5.0").maxBodySize(0).timeout(0).get();
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

    private Optional<Document> getCastDoc(Document doc) throws IOException {
        Optional<String> fullCastURL = getFullCastURL(doc);

        if (fullCastURL.isPresent()) {
            return Optional.of(Jsoup.connect(fullCastURL.get()).userAgent("Mozilla/5.0").maxBodySize(0).timeout(0).get());
        }

        return Optional.empty();
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
