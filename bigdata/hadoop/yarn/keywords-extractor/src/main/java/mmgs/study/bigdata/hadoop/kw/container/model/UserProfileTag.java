package mmgs.study.bigdata.hadoop.kw.container.model;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by Maria_Gromova on 8/30/2016.
 */
public class UserProfileTag {
    private Long id;
    private String keywordValue;
    private String keywordStatus;
    private String pricingType;
    private String keywordMatchType;
    private String destinationURL;

    UserProfileTag() {}

    void setId(Long id) {
        this.id = id;
    }

    void setKeywordValue(String keywordValue) {
        this.keywordValue = keywordValue;
    }

    void setKeywordStatus(String keywordStatus) {
        this.keywordStatus = keywordStatus;
    }

    void setPricingType(String pricingType) {
        this.pricingType = pricingType;
    }

    void setKeywordMatchType(String keywordMatchType) {
        this.keywordMatchType = keywordMatchType;
    }

    void setDestinationURL(String destinationURL) {
        this.destinationURL = destinationURL;
    }


    Long getId() {
        return id;
    }

    String getKeywordValue() {
        return keywordValue;
    }

    String getKeywordStatus() {
        return keywordStatus;
    }

    String getPricingType() {
        return pricingType;
    }

    String getKeywordMatchType() {
        return keywordMatchType;
    }

    void setKeywordValue(Set<String> keywords) {
        this.keywordValue = keywords.stream().collect(Collectors.joining(","));
    }

    public String getDestinationURL() {
        return this.destinationURL;
    }

}
