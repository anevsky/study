package mmgs.study.bigdata.spark.kwmatcher.utils;

import mmgs.study.bigdata.spark.kwmatcher.model.WeightedKeyword;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Utils {
    public static List<WeightedKeyword> mergeKeywords(List<WeightedKeyword> keywords1, List<WeightedKeyword> keywords2) {
        List<WeightedKeyword> keywords = new ArrayList<>();
        keywords.addAll(keywords1);
        keywords.addAll(keywords2);
        keywords.sort(Comparator.comparing(WeightedKeyword::getFrequency).reversed().thenComparing(WeightedKeyword::getKeyword));
        return new ArrayList<>(keywords.subList(0, Math.min(10, keywords1.size() + keywords2.size())));
    }
}
