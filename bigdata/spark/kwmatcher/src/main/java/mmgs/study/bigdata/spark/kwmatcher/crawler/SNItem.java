package mmgs.study.bigdata.spark.kwmatcher.crawler;

import mmgs.study.bigdata.spark.kwmatcher.tokenizer.KeywordsExtractor;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * POJO for Social Network event
 */
public class SNItem implements Serializable {
    private static final String ID = "id";
    private static final String DESCRIPTION = "description";

    private String id;
    private String description;

    public SNItem() {}

    public SNItem(JSONObject json) {
        this.id = json.getString(ID);
        this.description = json.getString(DESCRIPTION);
    }

    public static SNItem fromJSON(JSONObject json) {
        return new SNItem(json);
    }

    public static List<SNItem> fromJSON(JSONArray jsonArray) throws IOException {
        List<SNItem> snItems = new ArrayList<>();
        Iterator<Object> iterator = jsonArray.iterator();
        while (iterator.hasNext()) {
            SNItem snItem = new SNItem((JSONObject) iterator.next());
            KeywordsExtractor.getKeywordsList(snItem.description);
            snItems.add(snItem);
        }
        return Collections.unmodifiableList(snItems);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}