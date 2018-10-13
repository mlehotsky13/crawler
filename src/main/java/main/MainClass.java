package main;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import crawl.imdb.IMDBCrawler;

public class MainClass {
    public static void main(String[] args) throws IOException, InterruptedException {

        long start = System.currentTimeMillis();

        IMDBCrawler crawler = new IMDBCrawler();
        
        Path srcPath = Paths.get("/home/miroslav/Desktop/SKOLA/FIIT_STUBA/Ing/3.semester/VINF_I/imdb_pages_test");
        Path destPath = Paths.get("src/main/resources/data/imdb/parsed");

        // Path srcPath = Paths.get("src/main/resources/data/imdb/parsed/");
        // Path srcPath = Paths.get("src/main/resources/data/imdb/bulk/");
        // Path destPath = Paths.get("src/main/resources/data/imdb/bulk");

        // crawler.loadBulksToElastic(srcPath);
        // crawler.prepareBulkJsons(srcPath, destPath);
        crawler.parseAndSaveTitles(srcPath, destPath, 1_000_000);
        // crawler.crawlAndSave();

        long duration = System.currentTimeMillis() - start;

        System.out.println("Execution time: " + duration / 1000.0 + "s.");
    }
}
