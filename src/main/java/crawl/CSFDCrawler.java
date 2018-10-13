package crawl;

import static io.restassured.RestAssured.given;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import util.IOUtils;

public class CSFDCrawler extends AbstractCrawler {

    private static final String FILM_URL = "https://www.csfd.cz/film/REPLACE/prehled/";

    @Override
    public void crawlAndSave() throws InterruptedException {
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

        IOUtils.writeToFile(p, page);
    }
}
