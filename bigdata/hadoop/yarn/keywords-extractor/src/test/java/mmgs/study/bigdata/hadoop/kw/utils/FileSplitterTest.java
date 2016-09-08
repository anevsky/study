package mmgs.study.bigdata.hadoop.kw.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class FileSplitterTest {
    @Test
    public void splitTinyFile() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        FileSystem fileSystem = FileSystem.getLocal(conf);

        String fileName = "tiny-file-to-split.txt";
        URL testFile = getClass().getResource("/" + fileName);
        FileSplitter splitter = new FileSplitter();
        List<FileSplitter.FileChunk> chunks = splitter.split(fileSystem, testFile.getPath(), 2);

        checkSplits(fileName, chunks, Arrays.asList("tiny-splitted-file.txt"));
    }

    @Test
    public void splitNormalFile() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        FileSystem fileSystem = FileSystem.getLocal(conf);

        String fileName = "normal-file-to-split.txt";
        URL testFile = getClass().getResource("/" + fileName);
        FileSplitter splitter = new FileSplitter();
        List<FileSplitter.FileChunk> chunks = splitter.split(fileSystem, testFile.getPath(), 3);

        checkSplits(fileName, chunks, Arrays.asList("normal-splitted-file-1.txt", "normal-splitted-file-2.txt", "normal-splitted-file-3.txt"));
    }

    private void checkSplits(String sourceFile, List<FileSplitter.FileChunk> chunks, List<String> targetFiles) throws IOException {
        for (int i = 0; i < chunks.size(); i++) {
            BufferedReader source = asBufferedReader(getClass().getClassLoader().getResourceAsStream(sourceFile));
            FileSplitter.FileChunk chunk = chunks.get(i);
            String targetFile = targetFiles.get(i);
            BufferedReader target = asBufferedReader(getClass().getClassLoader().getResourceAsStream(targetFile));
            try {
                long start = chunk.getStart();
                source.skip(start);
                while (start < chunks.get(i).getEnd()) {
                    String sourceLine = source.readLine();
                    String targetLine = target.readLine();
                    if (sourceLine != null & targetLine != null) {
                        assertThat("Source file line matches target file line", sourceLine, is(targetLine));
                        start += sourceLine.length() + 1; // 0xA
                    }
                }
                assertThat("No lines in target files are left", 0L, is(target.lines().count()));
            } finally {
                target.close();
                source.close();
            }
        }
    }

    private BufferedReader asBufferedReader(InputStream in) throws IOException {
        return new BufferedReader(new InputStreamReader(in));
    }
}