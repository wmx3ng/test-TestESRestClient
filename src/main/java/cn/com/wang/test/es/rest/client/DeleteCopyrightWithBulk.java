package cn.com.wang.test.es.rest.client;

import cn.com.wang.test.es.rest.client.ResultEntity.CopyrightHits;
import cn.com.wang.test.es.rest.client.ResultEntity.CopyrightResult;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.lang3.StringUtils;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wang on 4:05 PM 1/25/17.
 * Version:
 * Description:
 */
public class DeleteCopyrightWithBulk {
    private static final Map<String, String> fieldReflect = new HashMap<String, String>();

    private static final String INDEX_NAME = "flume-patent";
    private static final String endpointForQuery = "/" + INDEX_NAME + "/_search";
    private static final String endpointForScrollQuery = "/_search/scroll";

    public static final Type LIST_TYPE5 = new TypeToken<CopyrightResult>() {
    }.getType();

    public static CopyrightResult resolveResult(final String json) {
        return new Gson().fromJson(json, LIST_TYPE5);
    }

    public static void main(String[] args) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(2000);
        sourceBuilder.query(QueryBuilders.matchAllQuery());

        System.out.println(sourceBuilder.toString());

        RestClient restClient = RestClient.builder(
                new HttpHost("hadoop1", 19200, "http"),
                new HttpHost("master", 19200, "http"),
                new HttpHost("hadoop2", 19200, "http"))
                .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                    public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                        return requestConfigBuilder.setConnectTimeout(5000)
                                .setSocketTimeout(30000);
                    }
                })
                .setMaxRetryTimeoutMillis(30000).build();

        try {
            HttpEntity entityForQuery = new NStringEntity(sourceBuilder.toString(), ContentType.APPLICATION_JSON);
            Map<String, String> paramsForQuery = new HashMap<String, String>();
            paramsForQuery.put("pretty", "true");
            paramsForQuery.put("scroll", "10m");

            Response response = restClient.performRequest("POST", endpointForQuery,
                    paramsForQuery, entityForQuery);

            Map<String, String> paramsForBulk = new HashMap<String, String>();
            paramsForBulk.put("pretty", "true");

            String json = EntityUtils.toString(response.getEntity());
            CopyrightResult result = resolveResult(json);
            while (!result.isEmpty()) {
                List<CopyrightHits.Hits> hits = result.getHits().getHits();
                System.out.println("scrollId:" + result.get_scroll_id());
                System.out.println("size:" + hits.size());
                StringBuilder strUpdate = new StringBuilder();
                if (!hits.isEmpty()) {
                    for (CopyrightHits.Hits hit : hits) {
                        String updateHead = resolveUpdateTag(hit.get_index(), hit.get_type(), hit.get_id());
                        strUpdate.append(updateHead);
                        strUpdate.append("\n");
                        String updateContent = resolveSource(hit.get_source());
                        strUpdate.append(updateContent);
                        strUpdate.append("\n");
                    }
                    System.out.println(strUpdate.toString());
                    Response updateResponse = restClient.performRequest("POST", "/_bulk",
                            paramsForBulk, new NStringEntity(strUpdate.toString(), ContentType.APPLICATION_JSON));
                }

                String scrollId = result.get_scroll_id();
                paramsForQuery.put("scroll_id", scrollId);
                response = restClient.performRequest("POST", endpointForScrollQuery,
                        paramsForQuery, entityForQuery);
                json = EntityUtils.toString(response.getEntity());
                result = resolveResult(json);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        try {
            restClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String resolveUpdateTag(String _index, String _type, String _id) {
        Map<String, String> meta = new HashMap<String, String>();
        meta.put("_index", _index);
        meta.put("_type", _type);
        meta.put("_id", _id);

        Map<String, Object> updateTag = new HashMap<String, Object>();
        updateTag.put("update", meta);

        return new Gson().toJson(updateTag);
    }

    private static String resolveSource(Map<String, Object> source) {
//        final String[] workCopyright = {"美术", "其他", "音乐"};
//        Set<String> workSet = new HashSet<String>() {
//            {
//                addAll(Arrays.asList(workCopyright));
//            }
//        };
        Map<String, Object> doc = new HashMap<String, Object>();
        Map<String, Object> updateFields = new HashMap<String, Object>();
//        if (source.containsKey("scc_category")) {
//            String category = String.valueOf(source.get("scc_category"));
//            if (workSet.contains(category)) {
//                updateFields.put("snc_copyright_type", "work_copyright");
//            } else {
//                updateFields.put("snc_copyright_type", "software_copyright");
//            }
//        } else {
//            updateFields.put("snc_copyright_type", "software_copyright");
//        }
        String field = "tfp_public_time";
//        String bakField = "tfp_publish_time";
        String targetField = "tfp_sort_time";
        Object value = getValueFromField(source, field);
        if (value != null) {
            updateFields.put(targetField, value);
        }
// else {
//            value = getValueFromField(source, bakField);
//            if (value != null) {
//                updateFields.put(targetField, value);
//            }
//        }

        doc.put("doc", updateFields);
        return new Gson().toJson(doc);
    }

    private static Object getValueFromField(Map<String, Object> source, String field) {
        if (source != null && source.containsKey(field)) {
            Object value = source.get(field);
            if (value != null && StringUtils.isNotEmpty(String.valueOf(value))) {
                return value;
            }
        }
        return null;
    }
}
