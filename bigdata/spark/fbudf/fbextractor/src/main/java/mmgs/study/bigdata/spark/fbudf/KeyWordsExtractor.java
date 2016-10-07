package mmgs.study.bigdata.spark.fbudf;

import org.apache.commons.lang3.math.NumberUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingInt;


public class KeyWordsExtractor {
    private static final Set<String> globalStopWords = new HashSet<>(Arrays.asList("", "a", "able", "about", "across", "after", "all", "almost", "also", "am", "among", "an", "and", "any", "are", "as", "at", "b", "be", "because", "been", "but", "by", "c", "can", "cannot", "class", "com", "could", "ctr", "d", "dear", "did", "div", "do", "does", "e", "either", "else", "ever", "every", "f", "for", "from", "g", "get", "google", "got", "h", "had", "has", "have", "he", "her", "hers", "him", "his", "how", "however", "i", "id", "if", "in", "input", "into", "is", "it", "its", "just", "label", "least", "let", "like", "likely", "ll", "login", "m", "may", "me", "might", "miniinthebox", "most", "must", "my", "name", "neither", "new", "no", "nor", "not", "of", "off", "often", "on", "only", "option", "or", "other", "our", "own", "p", "password", "r", "rather", "register", "republic", "s", "said", "say", "says", "she", "should", "since", "so", "some", "span", "t", "than", "that", "the", "their", "them", "then", "there", "these", "they", "this", "tis", "to", "too", "twas", "type", "us", "wants", "was", "we", "were", "what", "when", "where", "which", "while", "who", "whom", "why", "will", "with", "would", "yet", "you", "your", "ain", "aren", "can", "could", "couldn", "didn", "doesn", "don", "hasn", "he", "he", "he", "how", "how", "how", "i", "isn", "it", "might", "mightn", "must", "mustn", "shan", "she", "should", "shouldn", "that", "there", "they", "usd", "ve", "value", "wasn", "we", "weren", "what", "when", "where", "who", "why", "won", "would", "wouldn", "you"));

    public static Map<String, Integer> keywordsCount(String text) {
        return Arrays.stream(text.toLowerCase().split("[\\W_]+"))
                .filter(e -> !NumberUtils.isNumber(e)).filter(e -> !e.isEmpty())
                .filter(e -> !KeyWordsExtractor.globalStopWords.contains(e))
                .collect(groupingBy(Function.identity(), summingInt(e -> 1)));
    }
}
