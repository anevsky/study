package mmgs.study.bigdata.hadoop.kw.container;

import mmgs.study.bigdata.hadoop.kw.container.crawler.DocumentExtractor;
import mmgs.study.bigdata.hadoop.kw.container.crawler.WebDocumentExtractor;
import mmgs.study.bigdata.hadoop.kw.container.model.UserProfileTag;
import mmgs.study.bigdata.hadoop.kw.container.model.UserProfileTagParser;
import mmgs.study.bigdata.hadoop.kw.container.parser.HtmlKeyWordsExtractor;
import mmgs.study.bigdata.hadoop.kw.container.parser.KeyWordsExtractor;
import mmgs.study.bigdata.hadoop.kw.container.utils.KeyWordsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.jsoup.HttpStatusException;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Set;

public class Container {
    static final Logger LOG = Logger.getLogger(Container.class);

    public static void main(String[] args) {
        String inFilePath = args[0];
        String outFileDir = args[1];

        FileSystem fs;
        try {
            Path inFile = new Path(inFilePath);
            String fileName = inFile.getName();
            fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inFile)));

            FSDataOutputStream outFile = fs.create(new Path(args[1] + "/" + fileName + ".out"));
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outFile));
            String line;
            line = br.readLine();
            while (line != null) {
                if (UserProfileTagParser.isUserProfileTagHeader(line)) {
                    System.out.println("Header found");
                } else {
                    UserProfileTag userProfileTag = UserProfileTagParser.parseString(line);
                    System.out.println("Line parsed");
                    String destinationURL = userProfileTag.getDestinationURL();
                    System.out.println("Destination URL extracted " + destinationURL);
                    String keyWordsSource = "";
                    try {
                        DocumentExtractor htmlExtractor = new WebDocumentExtractor(destinationURL);
                        keyWordsSource = htmlExtractor.extractDocument();
                    } catch (HttpStatusException e) {
                        keyWordsSource = destinationURL;
                    }
                    System.out.println("HTML extracted " + keyWordsSource.length());
                    KeyWordsExtractor keyWordsExtractor = new HtmlKeyWordsExtractor();
                    List<String> keyWords = keyWordsExtractor.extract(keyWordsSource);
                    System.out.println("Keywords extracted " + keyWords.size());
                    Set<String> top10keyWords = KeyWordsUtils.top10(keyWords);
                    System.out.println("Top 10 keywords extracted " + top10keyWords.toString());
                    UserProfileTagParser.populateKeywordValue(userProfileTag, top10keyWords);
                    System.out.println("Top 10 keywords populated");
                    bw.write(UserProfileTagParser.userProfileTagAsString(userProfileTag));
                    System.out.println(UserProfileTagParser.userProfileTagAsString(userProfileTag));
                }
                line = br.readLine();
            }
            bw.flush();
            bw.close();
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
