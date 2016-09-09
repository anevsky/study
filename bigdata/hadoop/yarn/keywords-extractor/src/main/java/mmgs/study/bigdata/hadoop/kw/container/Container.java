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
    private static final Logger LOG = Logger.getLogger(Container.class);

    public static void main(String[] args) {
        String inFilePath = args[0];
        String outFileDir = args[1];
        Integer startPos = Integer.parseInt(args[2]);
        Integer endPos = Integer.parseInt(args[3]);

        LOG.info("Started processing characters from " + startPos + " till " + endPos);
        LOG.debug(inFilePath);
        LOG.debug(outFileDir);
        FileSystem fs;
        try {
            Path inFile = new Path(inFilePath);
            String fileName = inFile.getName();
            fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inFile)));

            FSDataOutputStream outFile = fs.create(new Path(args[1] + "/" + fileName + ".out." + startPos));
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outFile));
            String line;
            long charsRead = startPos;
            br.skip(charsRead);
            line = br.readLine();
            startPos += line.length();
            while (line != null && charsRead < endPos) {
                if (UserProfileTagParser.isUserProfileTagHeader(line)) {
                    LOG.debug("Header found");
                } else {
                    UserProfileTag userProfileTag = UserProfileTagParser.parseString(line);
                    LOG.debug("Line parsed");
                    String destinationURL = userProfileTag.getDestinationURL();
                    LOG.debug("Destination URL extracted " + destinationURL);
                    String keyWordsSource = "";
                    try {
                        DocumentExtractor htmlExtractor = new WebDocumentExtractor(destinationURL);
                        keyWordsSource = htmlExtractor.extractDocument();
                    } catch (HttpStatusException e) {
                        keyWordsSource = destinationURL;
                    }
                    LOG.debug("HTML extracted " + keyWordsSource.length());
                    KeyWordsExtractor keyWordsExtractor = new HtmlKeyWordsExtractor();
                    List<String> keyWords = keyWordsExtractor.extract(keyWordsSource);
                    LOG.debug("Keywords extracted " + keyWords.size());
                    Set<String> top10keyWords = KeyWordsUtils.top10(keyWords);
                    LOG.debug("Top 10 keywords extracted " + top10keyWords.toString());
                    UserProfileTagParser.populateKeywordValue(userProfileTag, top10keyWords);
                    LOG.debug("Top 10 keywords populated");
                    bw.write(UserProfileTagParser.userProfileTagAsString(userProfileTag));
                    LOG.debug(UserProfileTagParser.userProfileTagAsString(userProfileTag));
                }
                line = br.readLine();
                if (line != null)
                    startPos += line.length();
            }
            bw.flush();
            bw.close();
            br.close();
        } catch (Exception e) {
            LOG.info(e.getStackTrace());
        }
        finally {
            LOG.info("Finished processing characters from " + startPos + " till " + endPos);
        }
    }
}
