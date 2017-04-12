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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wang on 4:05 PM 1/25/17.
 * Version:
 * Description:
 */
public class ExactCopyrightChangeWithBulk {

    private static final String queryClause = "{\n" +
            "  \"size\": 1000,\n" +
            "  \"query\": {\n" +
            "    \"filtered\": {\n" +
            "      \"filter\": {\n" +
            "        \"bool\": {\n" +
            "          \"must\": [\n" +
            "            {\n" +
            "              \"term\": {\n" +
            "                \"sn_dataSource\": \"copyright_pledge_withdraw\"\n" +
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
    private static final String INDEX_TAG = "{ \"index\" : { \"_index\" : \"flume-copyright-other\", \"_type\" : \"flumetype\"} }";

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
                        Map<String, Object> source = hit.get_source();
                        strUpdate.append(INDEX_TAG);
                        strUpdate.append("\n");
                        strUpdate.append(new Gson().toJson(source));
                        strUpdate.append("\n");

                        strUpdate.append(generateDeleteInfo(hit.get_index(), hit.get_type(), hit.get_id()));
                        strUpdate.append("\n");
                    }
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

    private static String generateDeleteInfo(String _index, String _type, String _id) {
        Map<String, String> meta = new HashMap<String, String>();
        meta.put("_index", _index);
        meta.put("_type", _type);
        meta.put("_id", _id);

        Map<String, Object> deleteInfo = new HashMap<String, Object>();
        deleteInfo.put("delete", meta);

        return new Gson().toJson(deleteInfo);
    }

}
