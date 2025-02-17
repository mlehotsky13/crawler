package sk.stuba.fiit.crawl;

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

import sk.stuba.fiit.util.IOUtils;

public class IMDBCrawler implements Crawler {
    
    private static final ObjectMapper om = new ObjectMapper();

    private static final String BASE_URL = "https://www.imdb.com/";
    private static final String SEARCH_TITLE = "search/title";
    private static final String ALL_GENRES =
            "?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=b9121fa8-b7bb-4a3e-8887-aab822e0b5a7&pf_rd_r=4VPVFKZNBXANDZCFN972&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=moviemeter&explore=title_type,genres&page=111&ref_=adv_nxt";

    private static final List<String> NOT_WANTED_GENRES =
            Arrays.asList("Film-Noir", "Talk-Show", "News", "Reality-TV", "Musical", "Adult", "Short", "Game-Show");

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
                    es.submit(() -> new IMDBCrawler().crawlGenre(genreItem.text(), genreDoc, 1_000, 20));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        es.shutdown();
        es.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    private int crawlGenre(String genre, Document doc, int limitTitles, int limitPages) {
        int count = 0;
        int page = 1;

        System.out.println("Crawling genre " + genre + " ...");

        String nextPageURL = getNextPage(doc);
        while ((nextPageURL = getNextPage(doc)) != null && count < limitTitles && page <= limitPages) {
            try {
                doc = Jsoup.connect(nextPageURL).userAgent("Mozilla/5.0").maxBodySize(0).timeout(0).get();
                count += crawlGenrePage(doc, limitTitles);
                page++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return count;
    }

    private int crawlGenrePage(Document doc, int limit) {
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

        System.out.println("Crawled genre page with " + count + " titles.");

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

            IOUtils.writeToFile(Paths.get("src/main/resources/data/imdb/pages/", titleName + ".html"), doc.toString());

            if (castDoc.isPresent())
                IOUtils.writeToFile(Paths.get("src/main/resources/data/imdb/pages/", titleName + "_cast.html"),
                        castDoc.get().toString());
            if (summaryDoc.isPresent())
                IOUtils.writeToFile(Paths.get("src/main/resources/data/imdb/pages/", titleName + "_summary.html"),
                        summaryDoc.get().toString());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Optional<JsonNode> getTitleName(JsonNode node) {
        return Optional.ofNullable(node.get("name"));
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


    private String getNextPage(Document doc) {
        return Optional.ofNullable(doc.selectFirst("a[class=lister-page-next next-page]")).map(v -> v.attr("abs:href"))
                .orElse(null);
    }
}
