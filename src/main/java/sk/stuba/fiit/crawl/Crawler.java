package sk.stuba.fiit.crawl;

import java.io.IOException;

public interface Crawler {

    public abstract void crawlAndSave() throws IOException, InterruptedException;
}
