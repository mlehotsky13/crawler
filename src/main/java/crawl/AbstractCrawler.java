package crawl;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import org.apache.commons.io.FileUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbstractCrawler implements Crawler {

    protected static final ObjectMapper om = new ObjectMapper();

    protected void writeToFile(Path p, String s) {

        System.out.println("Writing to file " + p + " ...");

        try (BufferedWriter bw = Files.newBufferedWriter(p)) {
            bw.append(s);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    protected String readFile(Path p) {
        String result = "";

        System.out.println("Reading file " + p + " ...");

        try {
            result = FileUtils.readFileToString(p.toFile());
        } catch (IOException e) {
            System.out.println("Was not able to read file " + p + ".");
        }

        return result;
    }

    protected Optional<String> readFileOptional(Path p) {
        String result = readFile(p);
        return result != "" ? Optional.of(result) : Optional.empty();
    }
}
