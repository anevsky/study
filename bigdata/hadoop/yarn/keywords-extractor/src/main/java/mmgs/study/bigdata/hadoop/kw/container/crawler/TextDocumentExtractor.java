package mmgs.study.bigdata.hadoop.kw.container.crawler;

import java.io.IOException;

/**
 * Created by Maria_Gromova on 9/2/2016.
 */
public class TextDocumentExtractor implements DocumentExtractor {

    private String text;

    public TextDocumentExtractor(String text) {
        this.text = text;
    }

    @Override
    public String extractDocument() throws IOException {
        return text;
    }
}
