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
import org.elasticsearch.index.query.FilterBuilders;
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
public class ResolveCopyrightWithBulk {
    private static final Map<String, String> fieldReflect = new HashMap<String, String>();

    static {
//        fieldReflect.put("td_withdrawDate", "tfp_withdraw_time");
//        fieldReflect.put("sn_originRegistryType", "scc_register_type");
//        fieldReflect.put("sn_originRegistryNo", "snc_register_no");
//        fieldReflect.put("sc_workTitle", "scc_copyright_name");
//        fieldReflect.put("sn_no", "snc_enregister_no");
//        fieldReflect.put("sn_registryNo", "snc_register_no");
//        fieldReflect.put("sc_originSoftwareName", "scc_copyright_name");
//        fieldReflect.put("sn_version", "snc_version_no");
//        fieldReflect.put("sc_affairChg", "scc_change_item");
//        fieldReflect.put("sc_beforeChg", "scc_before_content");
//        fieldReflect.put("sc_afterChg", "scc_after_content");
//        fieldReflect.put("td_registryDate", "tfp_register_time");
//        fieldReflect.put("sc_affairChgSupp", "scc_change_supply_item");
//        fieldReflect.put("sc_beforeSupp", "scc_before_supply");
//        fieldReflect.put("sc_afterSupp", "scc_after_supply");
//        fieldReflect.put("sc_softwareName", "scc_copyright_name");
//        fieldReflect.put("sc_affairSupp", "scc_supply_item");
//        fieldReflect.put("sn_originRegistryPerson", "scc_owner");
//        fieldReflect.put("sc_withdrawCause", "scc_withdraw_cause");
//        fieldReflect.put("sn_softwareCopyrightRegistryNo", "snc_register_no");
//        fieldReflect.put("sc_assignor", "scc_assignor");
//        fieldReflect.put("sc_assignee", "scc_assignee");
//        fieldReflect.put("sc_assignee", "scc_assignee");
//        fieldReflect.put("sn_dataSource", "snc_copyright_type");
        fieldReflect.put("snc_classify_no", "scc_category");
    }


    private static final String INDEX_NAME = "flume-copyright";
    private static final String endpointForQuery = "/" + INDEX_NAME + "/_search";
    private static final String endpointForScrollQuery = "/_search/scroll";

    public static final Type LIST_TYPE5 = new TypeToken<CopyrightResult>() {
    }.getType();

    public static CopyrightResult resolveResult(final String json) {
        return new Gson().fromJson(json, LIST_TYPE5);
    }

    public static void main(String[] args) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(1000);
        sourceBuilder.query(QueryBuilders.filteredQuery(
                QueryBuilders.matchAllQuery(), FilterBuilders.boolFilter()
                        .must(FilterBuilders.termFilter("snc_copyright_type", "software_copyright"))
                        .mustNot(FilterBuilders.existsFilter("scc_category"))));

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
//                    System.out.println(strUpdate.toString());
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
        Map<String, Object> doc = new HashMap<String, Object>();
        Map<String, Object> updateFields = new HashMap<String, Object>();
        if (source != null && !source.isEmpty()) {
            for (Map.Entry<String, String> stringStringEntry : fieldReflect.entrySet()) {
                addField(updateFields, source, stringStringEntry.getKey(), stringStringEntry.getValue());
            }
        }
        doc.put("doc", updateFields);
        return new Gson().toJson(doc);
    }

    private static void addField(Map<String, Object> doc, Map<String, Object> source, String field, String newField) {
        Object content = getContentFromSource(source, field);
        if (content != null) {
            doc.put(newField, content);
        }
    }

    public static Object getContentFromSource(Map<String, Object> source, String field) {
        if (source.containsKey(field)) {
            if ("td_withdrawDate".equals(field) || "td_firstCreateDate".equals(field) || "td_registryDate".equals(field)
                    || "td_firstPublicDate".equals(field)) {
                String date = String.valueOf(source.get(field));
                if (date != null && !"null".equals(date) && date.length() == 10) {
                    return date + " 00:00:00";
                }

                return null;
            }
            return source.get(field);
        }

        return null;
    }

}
