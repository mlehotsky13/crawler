package main;

import java.io.IOException;

import crawl.Crawler;
import crawl.imdb.IMDBCrawler;

public class MainClass {
    public static void main(String[] args) throws IOException, InterruptedException {

        long start = System.currentTimeMillis();

        Crawler crawler = new IMDBCrawler();
        crawler.crawlAndSave();
        // crawler.downloadPages();

        long duration = System.currentTimeMillis() - start;

        System.out.println("Execution time: " + duration / 1000.0 + "s.");
    }
}
