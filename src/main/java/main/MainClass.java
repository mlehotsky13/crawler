package main;

import java.io.IOException;

import crawl.imdb.IMDBCrawler;

public class MainClass {
	public static void main(String[] args) throws IOException {
		IMDBCrawler crawler = new IMDBCrawler();
		crawler.crawlAndSave();
	}
}
