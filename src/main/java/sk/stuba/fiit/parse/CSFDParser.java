package sk.stuba.fiit.parse;

import java.io.IOException;
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

import sk.stuba.fiit.util.IOUtils;

public class CSFDParser implements Parser {

    private static final ObjectMapper om = new ObjectMapper();

    private static int count = 1;

    @Override
    public void parseAll() throws IOException {
        Path srcPath = Paths.get("/home/miroslav/Desktop/SKOLA/FIIT_STUBA/Ing/3.semester/VINF_I/csfd_pages");
        Path destPath = Paths.get("src/main/resources/data/csfd/parsed");

        parseAndSaveFilms(srcPath, destPath, 600_000);
    }

    public void parseAndSaveFilms(Path srcPath, Path destPath, int limit) throws IOException {
        ArrayNode films = om.createArrayNode();

        for (int i = 1; i < limit; i++) {
            String docString = IOUtils.readFile(srcPath.resolve("csfd_page" + i + ".html"));
            Document doc = Jsoup.parse(docString);

            parseFilmIfValid(doc).ifPresent(v -> films.add(v));

            if (films.size() > 999) {
                IOUtils.writeToFile(destPath.resolve("csfd_films_" + count + ".json"), films.toString());
                films.removeAll();
                count++;
            }
        }

        IOUtils.writeToFile(Paths.get("src/main/resources/data/csfd/parsed/csfd_films_" + count + ".json"), films.toString());
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
