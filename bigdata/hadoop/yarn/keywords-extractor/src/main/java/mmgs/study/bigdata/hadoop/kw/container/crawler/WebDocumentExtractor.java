package mmgs.study.bigdata.hadoop.kw.container.crawler;

import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by Maria_Gromova on 8/30/2016.
 */
public class WebDocumentExtractor implements DocumentExtractor {
    private static final String userAgent = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36";
    private static final int timeout = 5 * 1000;

    private URL url;

    public WebDocumentExtractor(String url) throws MalformedURLException, HttpStatusException {
        if (url == null || url.isEmpty())
            throw new IllegalArgumentException("URL cannot be empty");
        this.url = new URL(url);
    }

    @Override
    public String extractDocument() throws IOException {
        Document doc = Jsoup.connect(String.valueOf(url)).userAgent(userAgent).timeout(timeout).get();
        return doc.html();
    }
}
