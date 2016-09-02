package mmgs.study.bigdata.hadoop.kw.container.crawler;

import java.io.IOException;
import java.net.URI;

/**
 * Created by Maria_Gromova on 8/30/2016.
 */
public interface DocumentExtractor {
    String extractDocument() throws IOException;
}
