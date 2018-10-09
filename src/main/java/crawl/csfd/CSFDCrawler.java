package crawl.csfd;

import static io.restassured.RestAssured.given;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import crawl.AbstractCrawler;

public class CSFDCrawler extends AbstractCrawler {

    private static final String FILM_URL = "https://www.csfd.cz/film/REPLACE/prehled/";

    private static final ObjectMapper om = new ObjectMapper();

    private static final String PAGES_FOLDER = "/home/miroslav/Desktop/SKOLA/FIIT_STUBA/Ing/3.semester/VINF_I/csfd_pages";

    private static int count = 1;

    public void crawlAndSave() throws InterruptedException, JsonProcessingException {
        ArrayNode films = om.createArrayNode();

        for (int i = 1; i < 600_000; i++) {
            String docString = readFile(Paths.get(PAGES_FOLDER, "csfd_page" + i + ".html"));
            Document doc = Jsoup.parse(docString);

            parseFilmIfValid(doc).ifPresent(v -> films.add(v));

            if (films.size() > 999) {
                writeToFile(Paths.get("src/main/resources/data/csfd/parsed/csfd_films_" + count + ".json"), films.toString());
                films.removeAll();
                count++;
            }
        }

        writeToFile(Paths.get("src/main/resources/data/csfd/parsed/csfd_films_" + count + ".json"), films.toString());
    }

    public void downloadPages() throws InterruptedException {
        ExecutorService es = Executors.newCachedThreadPool();

        es.submit(() -> downloadPagesFromTo(500_000, 520_000));
        es.submit(() -> downloadPagesFromTo(520_000, 540_000));
        es.submit(() -> downloadPagesFromTo(540_000, 560_000));
        es.submit(() -> downloadPagesFromTo(560_000, 580_000));
        es.submit(() -> downloadPagesFromTo(580_000, 600_000));

        es.shutdown();
        es.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    private void downloadPagesFromTo(int from, int to) {
        for (int i = from; i < to; i++) {
            try {
                String pageUrl = FILM_URL.replace("REPLACE", String.valueOf(i));
                downloadPage(pageUrl, i);
            } catch (Exception e) {
            }
        }
    }

    private void downloadPage(String url, int i) {
        String page = given().get(url).then().extract().response().body().asString();
        Path p = Paths.get("src/main/resources/data/csfd/pages/csfd_page" + i + ".html");

        writeToFile(p, page);
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
        on.set("komentare", getFilmComments(doc));

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

        String[] countries = Optional.ofNullable(doc.selectFirst("p[class=origin]")).map(v -> v.text().split(",")[0].split("/"))
                .orElse(new String[0]);
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
        if (rating != null) {
            Optional.ofNullable(rating.selectFirst("h2[class=average]")).ifPresent(v -> on.put("average", v.ownText()));

            List<Element> meta = rating.select("meta");
            for (Element m : meta) {
                on.put(m.attr("itemprop"), m.attr("content"));
            }
        }

        return on;
    }

    private JsonNode getFilmComments(Document doc) {
        ArrayNode an = om.createArrayNode();

        Optional<Element> commentsDiv = Optional.ofNullable(doc.selectFirst("div[class=content comments]"));
        if (commentsDiv.isPresent()) {
            List<Element> comments = commentsDiv.get().select("li");
            for (Element comment : comments) {
                ObjectNode on = om.createObjectNode();
                on.put("autor", comment.selectFirst("h5[class=author]").selectFirst("a").ownText());
                on.put("datum", comment.selectFirst("span[class=date desc]").text().replaceAll("\\(|\\)", ""));
                on.put("obsah", comment.selectFirst("p[class=post]").text().replaceAll("\\(\\d+\\.\\d+\\.\\d+\\)$", ""));

                an.add(on);
            }
        }

        return an;
    }
}
