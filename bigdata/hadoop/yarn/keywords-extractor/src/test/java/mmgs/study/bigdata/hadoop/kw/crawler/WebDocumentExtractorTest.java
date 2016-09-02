package mmgs.study.bigdata.hadoop.kw.crawler;

import mmgs.study.bigdata.hadoop.kw.container.crawler.DocumentExtractor;
import mmgs.study.bigdata.hadoop.kw.container.crawler.WebDocumentExtractor;
import org.junit.Test;

import java.io.IOException;
import java.net.MalformedURLException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.*;

/**
 * Created by Maria_Gromova on 9/2/2016.
 */
public class WebDocumentExtractorTest {
    @Test(expected = IllegalArgumentException.class)

    public void createExtractorNullURL() {
        try {
            DocumentExtractor extractor = new WebDocumentExtractor(null);
        } catch (MalformedURLException e) {
            fail();
        }

    }

    @Test(expected = IllegalArgumentException.class)
    public void createExtractorEmptyURL() {
        try {
            DocumentExtractor extractor = new WebDocumentExtractor("");
        } catch (MalformedURLException e) {
            fail();
        }
    }

    @Test(expected = MalformedURLException.class)
    public void createExtractorMalformedURL() throws MalformedURLException {
        try {
            DocumentExtractor extractor = new WebDocumentExtractor("thisIsNot an URL");
        } catch (MalformedURLException e) {
            throw e;
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void createExtractorCorrectURL() {
        try {
            DocumentExtractor extractor = new WebDocumentExtractor("https://yandex.ru/");
            assertThat("New extractor object is created", extractor, is(notNullValue()));
        } catch (Exception e) {
            fail();
        }
    }

    @Test(expected = IOException.class)
    public void extractDocumentNotExistingURL() throws IOException {
        try {
            DocumentExtractor extractor = new WebDocumentExtractor("https://idontknow-thisurl-must-not-exist.ru/");
            String result = extractor.extractDocument();
            fail();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void extractDocumentCorrectURL() {
        try {
            DocumentExtractor extractor = new WebDocumentExtractor("http://example.com/");
            String result = extractor.extractDocument();
            assertThat("URL data is extracted correctly", true, is(result.equals(exampleURL)));
        } catch (Exception e) {
            fail();
        }
    }

    private String exampleURL =
            "<!doctype html>\n" +
            "<html>\n" +
            " <head> \n" +
            "  <title>Example Domain</title> \n" +
            "  <meta charset=\"utf-8\"> \n" +
            "  <meta http-equiv=\"Content-type\" content=\"text/html; charset=utf-8\"> \n" +
            "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"> \n" +
            "  <style type=\"text/css\">\n" +
            "    body {\n" +
            "        background-color: #f0f0f2;\n" +
            "        margin: 0;\n" +
            "        padding: 0;\n" +
            "        font-family: \"Open Sans\", \"Helvetica Neue\", Helvetica, Arial, sans-serif;\n" +
            "        \n" +
            "    }\n" +
            "    div {\n" +
            "        width: 600px;\n" +
            "        margin: 5em auto;\n" +
            "        padding: 50px;\n" +
            "        background-color: #fff;\n" +
            "        border-radius: 1em;\n" +
            "    }\n" +
            "    a:link, a:visited {\n" +
            "        color: #38488f;\n" +
            "        text-decoration: none;\n" +
            "    }\n" +
            "    @media (max-width: 700px) {\n" +
            "        body {\n" +
            "            background-color: #fff;\n" +
            "        }\n" +
            "        div {\n" +
            "            width: auto;\n" +
            "            margin: 0 auto;\n" +
            "            border-radius: 0;\n" +
            "            padding: 1em;\n" +
            "        }\n" +
            "    }\n" +
            "    </style> \n" +
            " </head> \n" +
            " <body> \n" +
            "  <div> \n" +
            "   <h1>Example Domain</h1> \n" +
            "   <p>This domain is established to be used for illustrative examples in documents. You may use this" +
            " domain in examples without prior coordination or asking for permission.</p> \n" +
            "   <p><a href=\"http://www.iana.org/domains/example\">More information...</a></p> \n" +
            "  </div>   \n" +
            " </body>\n" +
            "</html>" +
            "";
}