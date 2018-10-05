package crawl;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class AbstractCrawler implements Crawler {

    protected void writeToFile(Path p, String s) throws IOException {

        System.out.println("Writing to file ...");

        try (BufferedWriter bw = Files.newBufferedWriter(p)) {
            bw.append(s);
        }
    }
}
