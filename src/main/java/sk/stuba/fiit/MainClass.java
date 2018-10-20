package sk.stuba.fiit;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import sk.stuba.fiit.util.ElasticUtils;

public class MainClass {
    public static void main(String[] args) throws IOException, InterruptedException {

        long start = System.currentTimeMillis();

        // Crawler crawler = new IMDBCrawler();
        // Parser parser = new IMDBParser();

        // crawler.crawlAndSave();
        // parser.parseAll();
        
        ElasticUtils utils = new ElasticUtils();
        
        // Path srcPath = Paths.get("src/main/resources/data/imdb/parsed_test");
        // Path destPath = Paths.get("src/main/resources/data/imdb/bulk_test");

        // utils.prepareBulkJsons(srcPath, destPath);
        utils.loadBulksToElastic(Paths.get("src/main/resources/data/imdb/bulk"));

        long duration = System.currentTimeMillis() - start;

        System.out.println("Execution time: " + duration / 1000.0 + "s.");
    }
}
