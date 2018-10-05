package crawl.csfd;

import java.io.IOException;
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

import crawl.AbstractCrawler;

public class CSFDCrawler extends AbstractCrawler {

    private static final String FILM_URL = "https://www.csfd.cz/film/REPLACE/prehled/";

    private static final ObjectMapper om = new ObjectMapper();

    private static int count = 4;


    public void crawlAndSave() throws IOException {
        ArrayNode films = om.createArrayNode();

        for (int i = 3000; i < 658_762; i++) {
            try {
                Document doc = Jsoup.connect(FILM_URL.replaceFirst("REPLACE", String.valueOf(i))).get();
                films.add(parseFilm(doc));

                if (films.size() > 999) {
                    writeToFile(Paths.get("src/main/resources/data/csfd/csfd-films_" + count + ".json"), films.toString());
                    films.removeAll();
                    count++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        writeToFile(Paths.get("src/main/resources/data/csfd/csfd-films_" + count + ".json"), films.toString());
    }

    private void parseFilmLibrary(Document doc, int limit) throws IOException {
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

            if (an.size() >= 1000) {
                writeToFile(Paths.get("src/main/resources/data/csfd-films_" + count + ".json"), an.toString());
                an.removeAll();
                count++;
            }
        }

        System.out.println("Parsed film library with " + an.size() + " films.");
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

        System.out.println("Parsed page with " + an.size() + " films.");

        return an;
    }

    private Optional<JsonNode> parseFilmIfValid(Document doc) {
        return isWantedFilm(doc) ? Optional.of(parseFilm(doc)) : Optional.empty();
    }

    private boolean isWantedFilm(Document doc) {
        return Optional.ofNullable(doc.selectFirst("span[class=film-type]"))
                .map(v -> !Arrays.asList("(epizoda)", "(série)").contains(v.ownText())).orElse(true);
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
        on.set("rating", getFilmRating(doc));

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
        return Optional.ofNullable(doc.selectFirst("div[data-truncate=570]")).map(v -> v.text()).orElse("");
    }

    private ObjectNode getFilmRating(Document doc) {
        ObjectNode on = om.createObjectNode();

        Element rating = doc.selectFirst("div[id=rating]");
        List<Element> meta = rating.select("meta");
        on.put("average", rating.selectFirst("h2[class=average]").ownText());

        for (Element m : meta) {
            on.put(m.attr("itemprop"), m.attr("content"));
        }

        return on;
    }
}
