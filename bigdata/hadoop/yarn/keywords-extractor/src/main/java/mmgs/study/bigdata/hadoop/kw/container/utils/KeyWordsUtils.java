package mmgs.study.bigdata.hadoop.kw.container.utils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

/**
 * Created by Maria_Gromova on 9/1/2016.
 */
public class KeyWordsUtils {
    public static Set<String> topN(List<String> keyWords, int n) {
        Map<String, Long> wordCount = keyWords.stream().collect(groupingBy(Function.identity(), counting()));
        return wordCount.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(n).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())).keySet();
    }

    public static Set<String> top10(List<String> keyWords) {
        return KeyWordsUtils.topN(keyWords, 10);
    }

    public static String top10AsString(List<String> keyWords) {
        return KeyWordsUtils.top10(keyWords).stream().collect(Collectors.joining(","));
    }
}
