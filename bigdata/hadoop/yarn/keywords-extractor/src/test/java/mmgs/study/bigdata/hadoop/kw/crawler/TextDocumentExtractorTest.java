package mmgs.study.bigdata.hadoop.kw.crawler;

import mmgs.study.bigdata.hadoop.kw.container.crawler.DocumentExtractor;
import mmgs.study.bigdata.hadoop.kw.container.crawler.TextDocumentExtractor;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * Created by Maria_Gromova on 9/2/2016.
 */
public class TextDocumentExtractorTest {
    @Test
    public void extractDocumentSimple() throws Exception {
        String incomingText = "Some text to extract";
        DocumentExtractor extractor = new TextDocumentExtractor(incomingText);
        String result = extractor.extractDocument();
        assertThat("Incoming text exactly matches resulting text", true, is(incomingText.equals(result)));
    }
}