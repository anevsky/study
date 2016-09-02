package mmgs.study.bigdata.hadoop.kw.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

/**
 * Created by Maria_Gromova on 9/1/2016.
 */
public class HdfsManipulator {
    private final Configuration conf;

    private HdfsManipulator() {
        this.conf = new Configuration();
    }

    public static HdfsManipulator newInstance() {
        return new HdfsManipulator();
    }

    public void copyLocalToHdfs(String localFile, String hdfsFile) throws IOException {
        InputStream in = new BufferedInputStream(new FileInputStream(localFile));
        FileSystem fs = FileSystem.get(URI.create(hdfsFile), this.conf);
        OutputStream out = fs.create(new Path(hdfsFile), new Progressable() {
            public void progress() {
                System.out.print("*");
            }
        });
        IOUtils.copyBytes(in, out, 4096, true);
    }

    public void createHdfsDirectory(String newFolder) throws IOException {
        FileSystem hdfs = FileSystem.get(this.conf);
        Path newFolderPath = new Path(newFolder);
        if (hdfs.exists(newFolderPath)) {
            hdfs.delete(newFolderPath, true);
        }
        hdfs.mkdirs(newFolderPath);
    }

    public String getFS() {
        return this.conf.get("fs.defaultFS");
    }
}
