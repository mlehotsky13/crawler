package parse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
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
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import util.IOUtils;

public class IMDBParser implements Parser {
    
    private static final ObjectMapper om = new ObjectMapper();
    
    @Override
    public void parseAll() throws IOException {
        Path srcPath = Paths.get("/home/miroslav/Desktop/SKOLA/FIIT_STUBA/Ing/3.semester/VINF_I/imdb_pages");
        Path destPath = Paths.get("src/main/resources/data/imdb/parsed");
        
        parseAndSaveTitles(srcPath, destPath, 1_000_000);
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
                        IOUtils.writeToFile(Paths.get(destPath.toString(), "titles_" + mill + ".json"), an.toString());
                        an.removeAll();
                    }
                });

        IOUtils.writeToFile(Paths.get(destPath.toString(), "titles_" + System.currentTimeMillis() + ".json"), an.toString());
    }

    private JsonNode parseTitle(Path p) {
        ObjectNode on = om.createObjectNode();

        try {
            Document titleBaseDoc = Jsoup.parse(IOUtils.readFile(p));
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
        return parseOtherCast(doc, "Writing Credits");
    }

    private Optional<JsonNode> getDirectors(Optional<Document> castDoc) throws IOException {
        return castDoc.map(this::parseDirectors);
    }

    private ArrayNode parseDirectors(Document doc) {
        return parseOtherCast(doc, "Directed by", "Second Unit Director or Assistant Director");
    }

    private Optional<JsonNode> getProducers(Optional<Document> castDoc) throws IOException {
        return castDoc.map(this::parseProducers);
    }

    private ArrayNode parseProducers(Document doc) {
        return parseOtherCast(doc, "Produced by");
    }

    private Optional<JsonNode> getCamera(Optional<Document> castDoc) throws IOException {
        return castDoc.map(this::parseCamera);
    }

    private ArrayNode parseCamera(Document doc) {
        return parseOtherCast(doc, "Camera and Electrical Department");
    }

    private ArrayNode parseOtherCast(Document doc, String... headings) {
        ArrayNode an = om.createArrayNode();

        String selector = Arrays.asList(headings).stream().map(v -> "h4:contains(" + v + ")").collect(Collectors.joining(", "));
        List<Element> headingElements = doc.select(selector);
        List<Element> tables = headingElements.stream().map(Element::nextElementSibling).collect(Collectors.toList());

        for (Element table : tables) {
            List<Element> validRows = getValidOtherCastRows(table);
            for (Element row : validRows) {
                ObjectNode on = om.createObjectNode();

                on.put("name", row.selectFirst("td").text());
                on.put("url", row.selectFirst("td").selectFirst("a").attr("href"));
                on.put("credit", Optional.ofNullable(row.selectFirst("td[class=credit]")).map(Element::text).orElse(null));

                an.add(on);
            }
        }

        return an;
    }

    private Optional<Document> getCastDoc(Path p) throws IOException {
        return IOUtils.readFileOptional(p.resolveSibling(p.getFileName().toString().replaceAll(".html", "_cast.html")))
                .map(v -> Jsoup.parse(v));
    }

    private Optional<Document> getSummaryDoc(Path p) throws IOException {
        return IOUtils.readFileOptional(p.resolveSibling(p.getFileName().toString().replaceAll(".html", "_summary.html")))
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

    private List<Element> getValidOtherCastRows(Element element) {
        return element.select("tr").stream().filter(v -> v.selectFirst("td[class=name]") != null).collect(Collectors.toList());
    }

    private JsonNode getEmptyRating() {
        ObjectNode on = om.createObjectNode();

        on.set("ratingCount", null);
        on.set("bestRating", null);
        on.set("worstRating", null);
        on.set("ratingValue", null);

        return on;
    }
}
