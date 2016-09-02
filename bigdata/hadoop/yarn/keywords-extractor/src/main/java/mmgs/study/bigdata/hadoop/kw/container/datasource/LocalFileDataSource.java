package mmgs.study.bigdata.hadoop.kw.container.datasource;

import java.io.*;

/**
 * Created by Maria_Gromova on 8/30/2016.
 */
public class LocalFileDataSource implements FileDataSource {
    private BufferedReader reader;
    private BufferedWriter writer;

    private LocalFileDataSource(String fileName, Character openOption) throws IOException {
        switch (openOption) {
            case 'r': {
                this.reader = new BufferedReader(new FileReader(fileName));
                break;
            }
            case 'w': {
                this.writer = new BufferedWriter(new FileWriter(fileName));
                break;
            }
            default:
                throw new IllegalArgumentException("Invalid open option specified. Only 'r' or 'w' are acceptable");
        }
    }

    public static LocalFileDataSource newFileToRead(String fileName) throws IOException {
        return new LocalFileDataSource(fileName, 'r');
    }

    public static LocalFileDataSource newFileToWrite(String fileName) throws IOException {
        return new LocalFileDataSource(fileName, 'w');
    }

    @Override
    public String readLine() throws IOException {
        return reader.readLine();
    }

    @Override
    public void writeLine(String line) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }
}
