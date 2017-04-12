package cn.com.wang.test.es.rest.client;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * Created by wang on 2:32 PM 2/8/17.
 * Version:
 * Description:
 */
public class TestSearchSource {
    public static void main(String[] args) {
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();
        searchSourceBuilder.size(1000);
        searchSourceBuilder.query(QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("sn_dataSource", "work_copyright")));

        System.out.println("clause:" + searchSourceBuilder.toString());
    }
}
