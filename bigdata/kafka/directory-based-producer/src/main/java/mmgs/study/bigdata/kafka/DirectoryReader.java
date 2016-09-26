package mmgs.study.bigdata.kafka;


import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

// TODO: properly handle exceptions
public class DirectoryReader {

    private List<Path> files;
    private BufferedReader reader;
    private int iterator = 0;

    public DirectoryReader(String directory) throws IOException {
        files = listFiles(directory, "stream*");
        files.sort(Path::compareTo);
        initializeReader(files.get(0));
        iterator++;
    }

    private static List<Path> listFiles(String directory, String pattern) throws IOException {
        List<Path> files = new ArrayList<>();
        Path dir = Paths.get(directory);
        DirectoryStream<Path> stream = Files.newDirectoryStream(dir, pattern);
        for (Path entry : stream) {
            files.add(entry);
        }
        return files;
    }

    private void initializeReader(Path file) throws IOException {
        reader = Files.newBufferedReader(file);
    }

    // TODO: investigate if multi-threaded approach is required
    public String readLine() throws IOException {
        String line = reader.readLine();
        if (line == null) {
            if (iterator < files.size()) {
                initializeReader(files.get(iterator));
                iterator++;
            }
            line = reader.readLine();
        }
        return line;
    }

}
