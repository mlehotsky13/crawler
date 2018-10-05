package main;

import java.io.IOException;

import crawl.Crawler;
import crawl.csfd.CSFDCrawler;

public class MainClass {
    public static void main(String[] args) throws IOException, InterruptedException {

        long start = System.currentTimeMillis();

        Crawler crawler = new CSFDCrawler();
        crawler.crawlAndSave();

        long duration = System.currentTimeMillis() - start;

        System.out.println("Execution time: " + duration / 1000.0 + "s.");
    }
}
