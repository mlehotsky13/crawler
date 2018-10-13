package main;

import java.io.IOException;

import crawl.Crawler;
import crawl.IMDBCrawler;
import parse.IMDBParser;
import parse.Parser;

public class MainClass {
    public static void main(String[] args) throws IOException, InterruptedException {

        long start = System.currentTimeMillis();

        Crawler crawler = new IMDBCrawler();
        Parser parser = new IMDBParser();

        // crawler.crawlAndSave();
        parser.parseAll();

        long duration = System.currentTimeMillis() - start;

        System.out.println("Execution time: " + duration / 1000.0 + "s.");
    }
}
