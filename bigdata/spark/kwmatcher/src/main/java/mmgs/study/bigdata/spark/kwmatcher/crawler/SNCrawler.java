package mmgs.study.bigdata.spark.kwmatcher.crawler;

import mmgs.study.bigdata.spark.kwmatcher.model.TaggedClick;

import java.util.List;

public interface SNCrawler {
    List<SNItem> extractEvents(TaggedClick taggedClick, String connectionKey) throws Exception;
//    JSONObject extractPlace();
}
