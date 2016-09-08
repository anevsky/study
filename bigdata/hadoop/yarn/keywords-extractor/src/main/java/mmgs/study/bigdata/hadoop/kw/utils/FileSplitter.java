package mmgs.study.bigdata.hadoop.kw.utils;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileSplitter {
    private static final int MIN_CHUNK_SIZE = 1024; // 1K

    public class FileChunk {
        private long start;
        private long end;

        FileChunk (long start, long end) {
            this.start = start;
            this.end = end;
        }

        public long getEnd() {
            return this.end;
        }

        public long getStart() {
            return this.start;
        }
    }

    public List<FileChunk> split (FileSystem fileSystem, String file, int chunksAmt) throws IOException {
        Path path = new Path(file);
        FSDataInputStream dataInputStream = fileSystem.open(path);
        long fileLength = fileSystem.getFileStatus(path).getLen();
        long approxChunkSize = fileLength / chunksAmt;
        if (approxChunkSize < MIN_CHUNK_SIZE)
            approxChunkSize = fileLength;

        List<FileChunk> chunks = new ArrayList<>();
        long chunkStart = 0;
        byte currByte;
        long currPos = 0;
        while (currPos + approxChunkSize < fileLength) {
            dataInputStream.seek(currPos + approxChunkSize);
            do {
                currByte = dataInputStream.readByte();
                currPos = dataInputStream.getPos();
            } while (currPos != fileLength && currByte != 0xA);
            chunks.add(new FileChunk(chunkStart, currPos));

            chunkStart = currPos;
        }
        chunks.add(new FileChunk(currPos, fileLength));
        dataInputStream.close();
        return chunks;
    }
}
