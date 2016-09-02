package mmgs.study.bigdata.hadoop.kw.container.datasource;

import java.io.IOException;

/**
 * Created by Maria_Gromova on 8/30/2016.
 */
public interface FileDataSource {
    String readLine() throws IOException;

    void writeLine(String line);
}
