package crawl.imdb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
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
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import crawl.AbstractCrawler;

public class IMDBCrawler extends AbstractCrawler {

    private static final String BASE_URL = "https://www.imdb.com/";
    private static final String SEARCH_TITLE = "search/title";
    private static final String ALL_GENRES =
            "?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=b9121fa8-b7bb-4a3e-8887-aab822e0b5a7&pf_rd_r=4VPVFKZNBXANDZCFN972&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=moviemeter&explore=title_type,genres&page=111&ref_=adv_nxt";

    private static final List<String> NOT_WANTED_GENRES =
            Arrays.asList("Film-Noir", "Talk-Show", "News", "Reality-TV", "Musical", "Adult", "Short", "Game-Show");

    private static final ObjectMapper om = new ObjectMapper();
    
    private static int bulkId = 1;

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
                    es.submit(() -> new IMDBCrawler().parseGenre(genreItem.text(), genreDoc, 1_000, 20));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        es.shutdown();
        es.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    private int parseGenre(String genre, Document doc, int limitTitles, int limitPages) {
        int count = 0;
        int page = 1;

        System.out.println("Parsing genre " + genre + " ...");

        String nextPageURL = getNextPage(doc);
        while ((nextPageURL = getNextPage(doc)) != null && count < limitTitles && page <= limitPages) {
            try {
                doc = Jsoup.connect(nextPageURL).userAgent("Mozilla/5.0").maxBodySize(0).timeout(0).get();
                count += parseGenrePage(doc, limitTitles);
                page++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return count;
    }

    private int parseGenrePage(Document doc, int limit) {
        int count = 0;
        List<Element> titles = doc.select("div[class=lister-item mode-advanced]").stream().map(div -> div.selectFirst("a"))
                .collect(Collectors.toList());

        for (int i = 0; i < titles.size() && i < limit; i++) {
            try {
                doc = Jsoup.connect(titles.get(i).attr("abs:href")).userAgent("Mozilla/5.0").maxBodySize(0).timeout(0).get();
                downloadTitle(doc);
                count++;
            } catch (Exception e) {
                e.printStackTrace();
                i--;

                try {
                    Thread.currentThread().sleep(2000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }

        System.out.println("Parsed genre page with " + count + " titles.");

        return count;
    }

    private void downloadTitle(Document doc) {
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

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void parseAndSaveTitles(Path srcPath, Path destPath, int limit) throws IOException {
        ArrayNode an = om.createArrayNode();

        Files.walk(srcPath, 1)//
                .filter(v -> !Files.isDirectory(v) && !v.getFileName().toString().matches(".*_summary.html|.*_cast.html"))//
                .limit(limit)//
                .forEach(v -> {
                    an.add(parseTitle(v));

                    if (an.size() >= 1_000) {
                        long mill = System.currentTimeMillis();
                        writeToFile(Paths.get(destPath.toString(), "titles_" + mill + ".json"), an.toString());
                        an.removeAll();
                    }
                });

        writeToFile(Paths.get(destPath.toString(), "titles_" + System.currentTimeMillis() + ".json"), an.toString());
    }

    private JsonNode parseTitle(Path p) {
        ObjectNode on = om.createObjectNode();

        try {
            Document titleBaseDoc = Jsoup.parse(readFile(p));
            Element script = titleBaseDoc.selectFirst("script[type=application/ld+json]");
            JsonNode scriptNode = om.readTree(script.dataNodes().get(0).toString());

            on.set("name", getTitleName(scriptNode).orElse(null));
            on.set("url", getTitleUrl(scriptNode).orElse(null));
            on.set("contentRating", getTitleContentRating(scriptNode).orElse(null));
            on.set("type", getTitleType(scriptNode).orElse(null));
            on.set("publishDate", getTitlePublishDate(scriptNode).orElse(null));
            on.set("duration", getTitleDuration(scriptNode).orElse(null));
            on.set("budget", getTitleBudget(titleBaseDoc).orElse(null));
            on.set("rating", getTitleRating(scriptNode).orElseGet(this::getEmptyRating));
            on.set("genres", getTitleGenres(scriptNode).orElse(om.createArrayNode()));
            on.set("countries", getTitleCountries(titleBaseDoc).orElse(om.createArrayNode()));
            on.set("languages", getTitleLanguages(titleBaseDoc).orElse(null));
            on.set("keywords", getTitleKeywords(scriptNode).orElse(null));
            on.put("description", getTitleDescription(titleBaseDoc).orElse(null));
            on.put("storyline", getTitleStoryLine(titleBaseDoc).orElse(null));
            on.put("trivia", getTitleTrivia(titleBaseDoc).orElse(null));
            on.put("goofs", getTitleGoofs(titleBaseDoc).orElse(null));

            Optional<Document> castDoc = getCastDoc(p);
            on.set("cast", getCast(castDoc).orElse(om.createArrayNode()));
            on.set("writers", getWriters(castDoc).orElse(om.createArrayNode()));
            on.set("directors", getDirectors(castDoc).orElse(om.createArrayNode()));
            on.set("producers", getProducers(castDoc).orElse(om.createArrayNode()));
            on.set("cameraAndElectricalDepartment", getCamera(castDoc).orElse(om.createArrayNode()));

            Optional<Document> summaryDoc = getSummaryDoc(p);
            getSummary(summaryDoc).ifPresent(v -> on.put("description", v));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return on;
    }

    private Optional<JsonNode> getTitleName(JsonNode node) {
        return Optional.ofNullable(node.get("name"));
    }

    private Optional<JsonNode> getTitleUrl(JsonNode node) {
        return Optional.ofNullable(node.get("url"));
    }

    private Optional<JsonNode> getTitleContentRating(JsonNode node) {
        return Optional.ofNullable(node.get("contentRating"));
    }

    private Optional<JsonNode> getTitleType(JsonNode node) {
        return Optional.ofNullable(node.get("@type"));
    }

    private Optional<JsonNode> getTitlePublishDate(JsonNode node) {
        return Optional.ofNullable(node.get("datePublished"));
    }

    private Optional<JsonNode> getTitleDuration(JsonNode node) {
        return Optional.ofNullable(node.get("duration")).map(this::getTransformedDuration);
    }

    private JsonNode getTransformedDuration(JsonNode durationNode) {
        return new LongNode(Duration.parse(durationNode.asText()).toMillis());
    }

    private Optional<JsonNode> getTitleBudget(Document doc) {
        return Optional.ofNullable(doc.selectFirst("h4:contains(Budget:)")).map(v -> getTransformedBudget(v.parent()));
    }

    private JsonNode getTransformedBudget(Element element) {
        return new TextNode(element.ownText());
    }

    private Optional<JsonNode> getTitleRating(JsonNode node) {
        return Optional.ofNullable(node.get("aggregateRating")).map(this::getTransformedRating);
    }

    private JsonNode getTransformedRating(JsonNode ratingNode) {
        ObjectNode on = om.createObjectNode();

        Optional.ofNullable(ratingNode.get("ratingCount")).ifPresent(v2 -> on.put("ratingCount", v2.asInt()));
        Optional.ofNullable(ratingNode.get("bestRating")).ifPresent(v2 -> on.put("bestRating", v2.asDouble()));
        Optional.ofNullable(ratingNode.get("worstRating")).ifPresent(v2 -> on.put("worstRating", v2.asDouble()));
        Optional.ofNullable(ratingNode.get("ratingValue")).ifPresent(v2 -> on.put("ratingValue", v2.asDouble()));

        return on;
    }

    private Optional<JsonNode> getTitleGenres(JsonNode node) {
        return Optional.ofNullable(node.get("genre")).map(this::getTransformedTitleGenres);
    }

    private JsonNode getTransformedTitleGenres(JsonNode genresNode) {
        if (genresNode instanceof ArrayNode) {
            return genresNode;
        }

        return om.createArrayNode().add(genresNode.asText());
    }

    private Optional<JsonNode> getTitleCountries(Document doc) throws IOException {
        return Optional.ofNullable(doc.selectFirst("h4:contains(Country:)"))//
                .map(v -> getTransformedCountries(v.parent()));
    }

    private JsonNode getTransformedCountries(Element element) {
        return om.createArrayNode()
                .addAll(element.select("a").stream().map(v -> new TextNode(v.text())).collect(Collectors.toList()));
    }

    private Optional<JsonNode> getTitleLanguages(Document doc) throws IOException {
        return Optional.ofNullable(doc.selectFirst("h4:contains(Language:)"))//
                .map(v -> getTransformedLanguages(v.parent()));
    }

    private JsonNode getTransformedLanguages(Element element) {
        return om.createArrayNode()
                .addAll(element.select("a").stream().map(v -> new TextNode(v.text())).collect(Collectors.toList()));
    }

    private Optional<JsonNode> getTitleKeywords(JsonNode node) {
        return Optional.ofNullable(node.get("keywords"));
    }

    private Optional<String> getTitleDescription(Document doc) throws IOException {
        return Optional.ofNullable(doc.selectFirst("div[class=summary_text]")).map(v -> v.text());
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

    private Optional<String> getTitleStoryLine(Document doc) {
        return Optional.ofNullable(doc.selectFirst("div[id=titleStoryLine]"))//
                .map(v -> v.selectFirst("div[class=inline canwrap]"))//
                .map(v -> v.selectFirst("span"))//
                .map(v -> v.text());
    }

    private Optional<String> getTitleTrivia(Document doc) {
        return Optional.ofNullable(doc.selectFirst("div[id=trivia]")).map(v -> v.ownText().replaceAll("»", "").trim());
    }

    private Optional<String> getTitleGoofs(Document doc) {
        return Optional.ofNullable(doc.selectFirst("div[id=goofs]")).map(v -> v.ownText().replaceAll("»", "").trim());
    }

    private Optional<JsonNode> getCast(Optional<Document> castDoc) throws IOException {
        return castDoc.map(this::parseFullCast);
    }

    private ArrayNode parseFullCast(Document doc) {
        ArrayNode an = om.createArrayNode();

        Optional<Element> table = Optional.ofNullable(doc.selectFirst("table[class=cast_list]"));
        if (table.isPresent()) {
            List<Element> validCastRows = getValidCastRows(table.get());
            for (Element row : validCastRows) {
                ObjectNode on = om.createObjectNode();

                on.put("name", row.select("td").get(1).text());
                on.put("url", row.select("td").get(1).selectFirst("a").attr("href"));
                on.put("character", row.selectFirst("td[class=character]").text());

                an.add(on);
            }
        }

        return an;
    }

    private Optional<JsonNode> getWriters(Optional<Document> castDoc) throws IOException {
        return castDoc.map(this::parseWriters);
    }

    private ArrayNode parseWriters(Document doc) {
        ArrayNode an = om.createArrayNode();

        Optional<Element> writersHeading = Optional.ofNullable(doc.selectFirst("h4:contains(Writing Credits)"));
        Optional<Element> table = writersHeading.map(v -> v.nextElementSibling());
        if (table.isPresent()) {
            List<Element> validWritersRows = getValidWritersRows(table.get());
            for (Element row : validWritersRows) {
                ObjectNode on = om.createObjectNode();

                on.put("name", row.selectFirst("td").text());
                on.put("url", row.selectFirst("td").selectFirst("a").attr("href"));
                on.put("credit", Optional.ofNullable(row.selectFirst("td[class=credit]")).map(Element::text).orElse(null));

                an.add(on);
            }
        }

        return an;
    }

    private Optional<JsonNode> getDirectors(Optional<Document> castDoc) throws IOException {
        return castDoc.map(this::parseDirectors);
    }

    private ArrayNode parseDirectors(Document doc) {
        ArrayNode an = om.createArrayNode();

        List<Element> directorsHeadings =
                doc.select("h4:contains(Directed by), h4:contains(Second Unit Director or Assistant Director)");
        List<Element> tables = directorsHeadings.stream().map(Element::nextElementSibling).collect(Collectors.toList());
        for (Element table : tables) {
            List<Element> validDirectorsRows = getValidDirectorsRows(table);
            for (Element row : validDirectorsRows) {
                ObjectNode on = om.createObjectNode();

                on.put("name", row.selectFirst("td").text());
                on.put("url", row.selectFirst("td").selectFirst("a").attr("href"));
                on.put("credit", Optional.ofNullable(row.selectFirst("td[class=credit]")).map(Element::text).orElse(null));

                an.add(on);
            }
        }

        return an;
    }

    private Optional<JsonNode> getProducers(Optional<Document> castDoc) throws IOException {
        return castDoc.map(this::parseProducers);
    }

    private ArrayNode parseProducers(Document doc) {
        ArrayNode an = om.createArrayNode();

        Optional<Element> producersHeading = Optional.ofNullable(doc.selectFirst("h4:contains(Produced by)"));
        Optional<Element> table = producersHeading.map(v -> v.nextElementSibling());
        if (table.isPresent()) {
            List<Element> validProducersRows = getValidProducersRows(table.get());
            for (Element row : validProducersRows) {
                ObjectNode on = om.createObjectNode();

                on.put("name", row.selectFirst("td").text());
                on.put("url", row.selectFirst("td").selectFirst("a").attr("href"));
                on.put("credit", Optional.ofNullable(row.selectFirst("td[class=credit]")).map(Element::text).orElse(null));

                an.add(on);
            }
        }

        return an;
    }

    private Optional<JsonNode> getCamera(Optional<Document> castDoc) throws IOException {
        return castDoc.map(this::parseCamera);
    }

    private ArrayNode parseCamera(Document doc) {
        ArrayNode an = om.createArrayNode();

        Optional<Element> cameraHeading = Optional.ofNullable(doc.selectFirst("h4:contains(Camera and Electrical Department)"));
        Optional<Element> table = cameraHeading.map(v -> v.nextElementSibling());
        if (table.isPresent()) {
            List<Element> validProducersRows = getValidCameraRows(table.get());
            for (Element row : validProducersRows) {
                ObjectNode on = om.createObjectNode();

                on.put("name", row.selectFirst("td").text());
                on.put("url", row.selectFirst("td").selectFirst("a").attr("href"));
                on.put("credit", Optional.ofNullable(row.selectFirst("td[class=credit]")).map(Element::text).orElse(null));

                an.add(on);
            }
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


    private Optional<Document> getCastDoc(Path p) throws IOException {
        return readFileOptional(p.resolveSibling(p.getFileName().toString().replaceAll(".html", "_cast.html")))
                .map(v -> Jsoup.parse(v));
    }

    private Optional<Document> getSummaryDoc(Path p) throws IOException {
        return readFileOptional(p.resolveSibling(p.getFileName().toString().replaceAll(".html", "_summary.html")))
                .map(v -> Jsoup.parse(v));
    }

    private Optional<String> getSummary(Optional<Document> summaryDoc) throws IOException {
        if (summaryDoc.isPresent()) {
            return Optional.of(summaryDoc.get().selectFirst("h4[id=summaries]").nextElementSibling().selectFirst("p").text());
        }

        return Optional.empty();
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

    private List<Element> getValidCameraRows(Element element) {
        return element.select("tr").stream().filter(v -> v.selectFirst("td[class=name]") != null).collect(Collectors.toList());
    }

    public void prepareBulkJsons(Path srcDir, Path destDir) throws IOException {
        Files.walk(srcDir, 1)//
                .filter(v -> Files.isRegularFile(v))//
                .forEach(v -> {
                    try {
                        appendToFile(destDir.resolve("titles_bulk.json"), prepareBulkJson(v, destDir));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

    public String prepareBulkJson(Path srcPath, Path destPath) throws IOException {
        ArrayNode an = (ArrayNode) om.readTree(readFile(srcPath));
        StringBuilder sb = new StringBuilder();

        String bulkRow = "{ \"index\" : { \"_index\" : \"title\", \"_type\" : \"_doc\", \"_id\" : \"REPLACE\" } }";

        for (JsonNode n : an) {
            sb.append(bulkRow.replaceAll("REPLACE", String.valueOf(bulkId++)));
            sb.append("\n");
            sb.append(n.toString());
            sb.append("\n");
        }

        // String fileName = srcPath.getFileName().toString().replaceAll(".json", "");
        // appendToFile(destPath.resolve("titles_bulk.json"), sb.toString());
        
        return sb.toString();
    }

    private JsonNode getEmptyRating() {
        ObjectNode on = om.createObjectNode();

        on.set("ratingCount", null);
        on.set("bestRating", null);
        on.set("worstRating", null);
        on.set("ratingValue", null);

        return on;
    }

    private String getNextPage(Document doc) {
        return Optional.ofNullable(doc.selectFirst("a[class=lister-page-next next-page]")).map(v -> v.attr("abs:href"))
                .orElse(null);
    }
}
