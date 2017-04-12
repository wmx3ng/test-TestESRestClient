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

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wang on 4:05 PM 1/25/17.
 * Version:
 * Description:
 */
public class ScanCopyright {

    private static final String queryClause = "{\n" +
            "  \"size\": 1000,\n" +
            "  \"query\": {\n" +
            "    \"filtered\": {\n" +
            "      \"filter\": {\n" +
            "        \"bool\": {\n" +
            "          \"must\": [\n" +
            "            {\n" +
            "              \"term\": {\n" +
            "                \"sn_dataSource\": \"work_copyright\"\n" +
            "              }\n" +
            "            }\n" +
            "          ],\n" +
            "          \"must_not\": [\n" +
            "            {\n" +
            "              \"exists\": {\n" +
            "                \"field\": \"snc_copyright_type\"\n" +
            "              }\n" +
            "            }\n" +
            "          ]\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";

    private static final String endpointForQuery = "/flume-copyright/_search";
    private static final String endpointForScrollQuery = "/_search/scroll";

    public static final Type LIST_TYPE5 = new TypeToken<CopyrightResult>() {
    }.getType();

    public static CopyrightResult resolveResult(final String json) {
        return new Gson().fromJson(json, LIST_TYPE5);
    }

    public static void main(String[] args) {
        RestClient restClient = RestClient.builder(
                new HttpHost("master", 19200, "http"),
                new HttpHost("hadoop1", 19200, "http"),
                new HttpHost("hadoop2", 19200, "http"))
                .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                    public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                        return requestConfigBuilder.setConnectTimeout(5000)
                                .setSocketTimeout(30000);
                    }
                })
                .setMaxRetryTimeoutMillis(30000).build();

        try {
            HttpEntity entityForQuery = new NStringEntity(queryClause, ContentType.APPLICATION_JSON);
            Map<String, String> paramsForQuery = new HashMap<String, String>();
            paramsForQuery.put("pretty", "true");
            paramsForQuery.put("scroll", "10m");

            Response response = restClient.performRequest("POST", endpointForQuery,
                    paramsForQuery, entityForQuery);

            Map<String, String> paramsForUpdate = new HashMap<String, String>();
            paramsForUpdate.put("pretty", "true");

            String json = EntityUtils.toString(response.getEntity());
            CopyrightResult result = resolveResult(json);
            while (!result.isEmpty()) {
                List<CopyrightHits.Hits> hits = result.getHits().getHits();
                System.out.println("scrollId:" + result.get_scroll_id());
                System.out.println("size:" + hits.size());
                if (!hits.isEmpty()) {
                    for (CopyrightHits.Hits hit : hits) {
                        Map<String, Object> source = hit.get_source();
//                        final Map<String, Object> doc = resolveSourceOfSoftwareCopyright(source);
                        final Map<String, Object> doc = resolveSourceOfWorkCopyright(source);
                        String endpointForUpdate = "/" + hit.get_index()
                                + "/" + hit.get_type()
                                + "/" + hit.get_id()
                                + "/" + "_update";
                        Map<String, Object> map = new HashMap<String, Object>();
                        map.put("doc", doc);
                        String updateDoc = new Gson().toJson(map);
                        System.out.println(updateDoc);
                        Response updateResponse = restClient.performRequest("POST", endpointForUpdate,
                                paramsForUpdate, new NStringEntity(updateDoc, ContentType.APPLICATION_JSON));
                    }
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

    public static Object getContentFromSource(Map<String, Object> source, String field) {
        if (source.containsKey(field)) {
            if ("td_firstCreateDate".equals(field) || "td_registryDate".equals(field)
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

    private static void addField(Map<String, Object> doc, Map<String, Object> source, String field, String newField) {
        Object content = getContentFromSource(source, field);
        if (content != null) {
            doc.put(newField, content);
        }
    }

    public static Map<String, Object> resolveSourceOfWorkCopyright(Map<String, Object> source) {
        if (source == null || source.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Object> doc = new HashMap<String, Object>();
        Map<String, String> fieldReflect = new HashMap<String, String>();
        fieldReflect.put("sn_registryNo", "snc_register_no");
        fieldReflect.put("td_firstCreateDate", "tfp_finish_time");
        fieldReflect.put("td_registryDate", "tfp_register_time");
        fieldReflect.put("sc_copyrightHolder", "scc_owner");
        fieldReflect.put("sc_workType", "scc_category");
        fieldReflect.put("sc_workTitle", "scc_copyright_name");
        fieldReflect.put("td_firstPublicDate", "tfp_publish_time");
        fieldReflect.put("sn_dataSource", "snc_copyright_type");

        for (Map.Entry<String, String> stringStringEntry : fieldReflect.entrySet()) {
            addField(doc, source, stringStringEntry.getKey(), stringStringEntry.getValue());
        }

        return doc;
    }

    public static Map<String, Object> resolveSourceOfSoftwareCopyright(Map<String, Object> source) {
        if (source == null || source.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Object> doc = new HashMap<String, Object>();
        Map<String, String> fieldReflect = new HashMap<String, String>();
        fieldReflect.put("sn_province", "sca_province");
        fieldReflect.put("sn_typeNo", "snc_classify_no");
        fieldReflect.put("sn_registryNo", "snc_register_no");
        fieldReflect.put("sc_softwareFullName", "scc_copyright_name");
        fieldReflect.put("sc_softwareSimpleName", "scc_copyright_short_name");
        fieldReflect.put("td_registryDate", "tfp_register_time");
        fieldReflect.put("sn_city", "sca_city");
        fieldReflect.put("sc_copyrightHolder", "scc_owner");
        fieldReflect.put("td_firstPublicDate", "tfp_publish_time");
        fieldReflect.put("sn_version", "snc_version_no");
        fieldReflect.put("sn_country", "sca_country");
        fieldReflect.put("sn_dataSource", "snc_copyright_type");

        for (Map.Entry<String, String> stringStringEntry : fieldReflect.entrySet()) {
            addField(doc, source, stringStringEntry.getKey(), stringStringEntry.getValue());
        }

        return doc;
    }
}
