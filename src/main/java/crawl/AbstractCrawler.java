package crawl;

import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbstractCrawler implements Crawler {

    protected static final ObjectMapper om = new ObjectMapper();

}