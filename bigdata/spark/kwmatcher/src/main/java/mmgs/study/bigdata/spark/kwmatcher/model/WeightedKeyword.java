package mmgs.study.bigdata.spark.kwmatcher.model;

import mmgs.study.bigdata.spark.kwmatcher.tokenizer.CardKeyword;

import java.io.Serializable;

/**
 * Weighted keyword
 */
public class WeightedKeyword implements Serializable {
    private String keyword;
    private long frequency;

    public WeightedKeyword() {
    }

    public WeightedKeyword(String keyword, long frequency) {
        this.keyword = keyword;
        this.frequency = frequency;
    }

    public WeightedKeyword(WeightedKeyword keyword) {
        this.keyword = keyword.getKeyword();
        this.frequency = keyword.getFrequency();
    }

    public WeightedKeyword(CardKeyword cardKeyword) {
        this.keyword = cardKeyword.getTerms().iterator().next();
        this.frequency = cardKeyword.getFrequency();
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public long getFrequency() {
        return frequency;
    }

    public void setFrequency(long frequency) {
        this.frequency = frequency;
    }
}
