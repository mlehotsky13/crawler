package crawl.csfd;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import crawl.Crawler;

public class CSFDCrawler implements Crawler {

    private static final String BASE_URL =
            "https://www.csfd.cz/filmoteky/?format%5B%5D=0&quality%5B%5D=0&genre%5B%5D=0&film=&user=&ok=Zobrazit&_form_=collection";

    private static final ObjectMapper om = new ObjectMapper();

    public void crawlAndSave() throws IOException {
        Document doc = Jsoup.connect(BASE_URL).userAgent("Mozilla/5.0").timeout(0).get();
        ArrayNode films = parseFilmLibrary(doc, 150);

        writeToFile(Paths.get("src/main/resources/data/csfd-films.json"), films.toString());
    }

    private ArrayNode parseFilmLibrary(Document doc, int limit) throws IOException {
        ArrayNode an = om.createArrayNode();

        an.addAll(parsePage(doc, limit));
        while (an.size() < limit) {
            Optional<String> nextPageURL =
                    Optional.ofNullable(doc.selectFirst("a[class=button]:contains(další)")).map(v -> v.attr("abs:href"));

            if (!nextPageURL.isPresent()) {
                break;
            }

            Document nextPage = Jsoup.connect(nextPageURL.get()).userAgent("Mozilla/5.0").timeout(0).get();
            an.addAll(parsePage(nextPage, limit - an.size()));
        }

        return an;
    }

    private ArrayNode parsePage(Document doc, int limit) throws IOException {
        ArrayNode an = om.createArrayNode();

        Element table = doc.selectFirst("table[class=ui-table-list]");
        List<Element> rows = table.select("tr");

        for (int i = 1; i < rows.size() && i <= limit; i++) {
            String filmURL = rows.get(i).selectFirst("a").attr("abs:href");
            Document filmDoc = Jsoup.connect(filmURL).userAgent("Mozilla/5.0").timeout(0).get();

            parseFilmIfValid(filmDoc).ifPresent(v -> an.add(v));
        }

        return an;
    }

    private Optional<JsonNode> parseFilmIfValid(Document doc) {
        return isWantedFilm(doc) ? Optional.of(parseFilm(doc)) : Optional.empty();
    }

    private boolean isWantedFilm(Document doc) {
        return !Arrays.asList("(epizoda)", "(série)").contains(doc.selectFirst("span[class=film-type]").ownText());
    }

    private JsonNode parseFilm(Document doc) {
        ObjectNode on = om.createObjectNode();

        on.put("nazov", getFilmName(doc));
        on.set("zanre", getFilmGenres(doc));
        on.set("krajiny", getFilmCountries(doc));
        on.put("rok", getFilmYear(doc));
        on.put("trvanie", getFilmDuration(doc));
        on.set("rezia", getFilmDirectors(doc));
        on.set("scenar", getFilmScenarists(doc));
        on.set("hraju", getFilmActors(doc));
        on.put("obsah", getFilmContent(doc));

        System.out.println("Parsed film");

        return on;
    }

    private String getFilmName(Document doc) {
        return Optional.ofNullable(doc.selectFirst("h1[itemprop=name]")).map(v -> v.ownText()).orElse("");
    }

    private JsonNode getFilmGenres(Document doc) {
        ArrayNode an = om.createArrayNode();

        String[] genres =
                Optional.ofNullable(doc.selectFirst("p[class=genre]")).map(v -> v.text().split("/")).orElse(new String[0]);
        for (String genre : genres) {
            an.add(genre.trim());
        }

        return an;
    }

    private JsonNode getFilmCountries(Document doc) {
        ArrayNode an = om.createArrayNode();

        String[] countries = doc.selectFirst("p[class=origin]").text().split(",")[0].split("/");
        for (String country : countries) {
            an.add(country.trim());
        }

        return an;
    }

    private String getFilmYear(Document doc) {
        return Optional.ofNullable(doc.selectFirst("span[itemprop=dateCreated]")).map(v -> v.text()).orElse("");
    }

    private String getFilmDuration(Document doc) {
        String[] tokens =
                Optional.ofNullable(doc.selectFirst("p[class=origin]")).map(v -> v.text().split(",")).orElse(new String[0]);

        return tokens.length == 3 ? tokens[tokens.length - 1].replaceAll("\\D", "").trim() : "";
    }

    private JsonNode getFilmDirectors(Document doc) {
        ArrayNode an = om.createArrayNode();

        Optional<Element> preceedingTitle = Optional.ofNullable(doc.selectFirst("h4:contains(Režie:)"));
        preceedingTitle.ifPresent(v -> {
            List<Element> directors = v.nextElementSibling().select("a");
            for (Element director : directors) {
                an.add(director.ownText());
            }
        });

        return an;
    }

    private JsonNode getFilmScenarists(Document doc) {
        ArrayNode an = om.createArrayNode();

        Optional<Element> preceedingTitle = Optional.ofNullable(doc.selectFirst("h4:contains(Scénář:)"));
        preceedingTitle.ifPresent(v -> {
            List<Element> scenarists = v.nextElementSibling().select("a");
            for (Element scenarist : scenarists) {
                an.add(scenarist.ownText());
            }
        });

        return an;
    }

    private JsonNode getFilmActors(Document doc) {
        ArrayNode an = om.createArrayNode();

        Optional<Element> preceedingTitle = Optional.ofNullable(doc.selectFirst("h4:contains(Hrají:)"));
        preceedingTitle.ifPresent(v -> {
            List<Element> actors = v.nextElementSibling().select("a");
            for (Element actor : actors) {
                an.add(actor.ownText());
            }
        });

        return an;
    }

    private String getFilmContent(Document doc) {
        return Optional.ofNullable(doc.selectFirst("div[data-truncate=570]")).map(v -> v.ownText()).orElse("");
    }

    private void writeToFile(Path p, String s) throws IOException {

        System.out.println("Writing to file ...");

        try (BufferedWriter bw = Files.newBufferedWriter(p)) {
            bw.append(s);
        }
    }

}
