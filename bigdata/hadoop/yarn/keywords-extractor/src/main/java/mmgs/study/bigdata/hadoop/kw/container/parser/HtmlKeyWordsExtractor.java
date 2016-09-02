package mmgs.study.bigdata.hadoop.kw.container.parser;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.NodeTraversor;
import org.jsoup.select.NodeVisitor;

import java.util.*;
import java.util.stream.Collectors;

import static org.jsoup.helper.StringUtil.isNumeric;

/**
 *
 */
public class HtmlKeyWordsExtractor implements KeyWordsExtractor {
    // TODO: add domain to stopwords list
    private static final Set<String> globalStopWords = new HashSet<>(Arrays.asList("", "a", "able", "about", "across", "after", "all", "almost", "also", "am", "among", "an", "and", "any", "are", "as", "at", "b", "be", "because", "been", "but", "by", "c", "can", "cannot", "com", "could", "d", "dear", "did", "do", "does", "e", "either", "else", "ever", "every", "f", "for", "from", "g", "get", "got", "h", "had", "has", "have", "he", "her", "hers", "him", "his", "how", "however", "i", "if", "in", "into", "is", "it", "its", "just", "least", "let", "like", "likely", "ll", "m", "may", "me", "might", "most", "must", "my", "neither", "no", "nor", "not", "of", "off", "often", "on", "only", "or", "other", "our", "own", "r", "rather", "s", "said", "say", "says", "she", "should", "since", "so", "some", "t", "than", "that", "the", "their", "them", "then", "there", "these", "they", "this", "tis", "to", "too", "twas", "us", "wants", "was", "we", "were", "what", "when", "where", "which", "while", "who", "whom", "why", "will", "with", "would", "yet", "you", "your", "ain", "aren", "can", "could", "couldn", "didn", "doesn", "don", "hasn", "he", "he", "he", "how", "how", "how", "i", "i", "i", "i", "isn", "it", "might", "mightn", "must", "mustn", "shan", "she", "she", "she", "should", "shouldn", "that", "there", "they", "ve", "wasn", "we", "weren", "what", "when", "where", "who", "why", "won", "would", "wouldn", "you"));
    private List<String> words = new ArrayList<>();

    @Override
    public List<String> extract(String text) {
        Document doc = Jsoup.parse(text).normalise();
        getWordsMap(doc.head());
        getWordsMap(doc.body());
        return getWords();
    }

    private void getWordsMap(Element element) {
        CountingVisitor counter = new CountingVisitor();
        NodeTraversor traversor = new NodeTraversor(counter);
        traversor.traverse(element);
    }

    private class CountingVisitor implements NodeVisitor {

        // hit when the node is first seen
        public void head(Node node, int depth) {
            if (node instanceof TextNode)
                if (!((TextNode) node).isBlank())
                    addWords(((TextNode) node).text().toLowerCase());
        }

        // hit when all of the node's children (if any) have been visited
        public void tail(Node node, int depth) {
        }

        private void addWords(String text) {
            String[] words = text.split("[\\W_]+");
            HtmlKeyWordsExtractor.this.words.addAll(Arrays.asList(words).stream()
                    .filter(e -> !isNumeric(e)).filter(e -> !e.isEmpty())
                    .filter(e -> !HtmlKeyWordsExtractor.globalStopWords.contains(e))
                    .collect(Collectors.toList())
            );
        }
    }

    private List<String> getWords() {
        return Collections.unmodifiableList(words);
    }
}
