package mmgs.study.bigdata.hadoop.kw.container.model;

import java.util.IllegalFormatException;
import java.util.Set;

/**
 * Created by Maria_Gromova on 9/2/2016.
 */
public final class UserProfileTagParser {
    public static UserProfileTag parseString(String line) throws IllegalFormatException {
        UserProfileTag userProfileTag = new UserProfileTag();
        String[] attrs = line.split("\\t");
        userProfileTag.setId(Long.parseLong(attrs[0]));
        userProfileTag.setKeywordValue(attrs[1]);
        userProfileTag.setKeywordStatus(attrs[2]);
        userProfileTag.setPricingType(attrs[3]);
        userProfileTag.setKeywordMatchType(attrs[4]);
        userProfileTag.setDestinationURL(attrs[5]);
        return userProfileTag;
    }

    public static boolean isUserProfileTagHeader(String line) {
        return "ID\tKeyword Value\tKeyword Status\tPricing Type\tKeyword Match Type\tDestination URL".equals(line);
    }

    public static UserProfileTag populateKeywordValue(UserProfileTag userProfileTag, Set<String> keywords) {
        userProfileTag.setKeywordValue(keywords);
        return userProfileTag;
    }

    public static String userProfileTagAsString(UserProfileTag userProfileTag) {
        StringBuilder sb = new StringBuilder();
        sb.append(userProfileTag.getId().toString()).append("\t")
                .append(userProfileTag.getKeywordValue()).append("\t")
                .append(userProfileTag.getKeywordStatus()).append("\t")
                .append(userProfileTag.getPricingType()).append("\t")
                .append(userProfileTag.getKeywordMatchType()).append("\t")
                .append(userProfileTag.getDestinationURL());
        return sb.toString();
    }
}
