package mmgs.study.bigdata.hadoop.kw.utils;

import mmgs.study.bigdata.hadoop.kw.container.utils.KeyWordsUtils;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertThat;

/**
 * Created by mash on 8/29/16.
 */

public class KeyWordsUtilsTest {

    private List<String> initListNoDuplicates(Integer n) {
        List<String> keyWords = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            keyWords.add("word" + i);
        }
        return keyWords;
    }

    @Test
    public void topNExactMatch() {
        Integer n = 10;
        List<String> keyWords = initListNoDuplicates(n);
        Set<String> topKeyWords = KeyWordsUtils.topN(keyWords, n);
        assertThat("Top 10 keyword list is of size 10", n, equalTo(topKeyWords.size()));
        assertThat("Top 10 keyword list equals initial keywords map", keyWords, everyItem(isIn(topKeyWords)));
    }

    @Test
    public void topNLessThanN() {
        Integer n = 5;
        List<String> keyWords = initListNoDuplicates(n);
        Set<String> topKeyWords = KeyWordsUtils.topN(keyWords, n);
        assertThat("Top " + n + " keyword list is of size " + n, n, equalTo(topKeyWords.size()));
        assertThat("Top " + n + " keyword list equals initial keywords map",topKeyWords , everyItem(isIn(keyWords)));
    }

    @Test
    public void topNMoreThanN() {
        Integer n = 5;
        List<String> keyWords = initListNoDuplicates(n);
        keyWords.addAll(initListNoDuplicates(n * 2));
        List<String> expectedKeyWords = initListNoDuplicates(n);
        Set<String> topKeyWords = KeyWordsUtils.topN(keyWords, n);
        assertThat("Top " + n + " keyword list is of size " + n, n, equalTo(topKeyWords.size()));
        assertThat("Top " + n + " keyword list equals initial keywords map", topKeyWords, everyItem(isIn(keyWords)));
        assertThat("Top keywords are in list of expected top", topKeyWords, everyItem(isIn(expectedKeyWords)));
    }

    //TODO: what is to be returned in case of top3({word1, word1, word2, word2, word3, word4, word5}) ?
}
