package mmgs.study.bigdata.hadoop.kw.parser;

import mmgs.study.bigdata.hadoop.kw.container.parser.HtmlKeyWordsExtractor;
import mmgs.study.bigdata.hadoop.kw.container.parser.KeyWordsExtractor;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.*;

/**
 * Created by Maria_Gromova on 9/2/2016.
 */
public class HtmlKeyWordsExtractorTest {
    @Test
    public void extractSimple() throws Exception {
        String html = "<head>Very serious document<head>" +
                "<body>Very serious document is about testing very serious document</body>";
        KeyWordsExtractor extractor = new HtmlKeyWordsExtractor();
        List<String> keyWords = extractor.extract(html);
        List<String> expectedKeyWords = Arrays.asList("very", "serious", "document"
                ,"very", "serious", "document", "testing", "very", "serious", "document");
        assertThat("Keywords match", expectedKeyWords, everyItem(isIn(keyWords)));
        assertThat("Keywords match", keyWords, everyItem(isIn(expectedKeyWords)));
    }

}