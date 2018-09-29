package main;

import java.io.IOException;

import crawl.imdb.IMDBCrawler;

public class MainClass {
	public static void main(String[] args) throws IOException, InterruptedException {
		
		long start = System.currentTimeMillis();
		
		IMDBCrawler crawler = new IMDBCrawler();
		crawler.crawlAndSave();
		
		long duration = System.currentTimeMillis() - start;
		
		System.out.println("Execution time: " + duration / 1000.0 + "s.");
	}
}
