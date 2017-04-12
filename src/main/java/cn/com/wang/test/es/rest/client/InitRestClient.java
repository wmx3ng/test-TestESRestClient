package cn.com.wang.test.es.rest.client;

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
import java.util.Collections;

/**
 * Created by wang on 4:05 PM 1/25/17.
 * Version:
 * Description:
 */
public class InitRestClient {
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



        Response response = null;
        try {
            final String clause = "{\n" +
                    "    \"size\":0,\n" +
                    "    \"aggs\":{\n" +
                    "        \"copyrightType\":{\n" +
                    "            \"terms\":{\n" +
                    "                \"field\":\"sn_dataSource\",\n" +
                    "                \"size\":0\n" +
                    "            }\n" +
                    "        }\n" +
                    "    }\n" +
                    "}";
            HttpEntity entity = new NStringEntity(clause, ContentType.APPLICATION_JSON);
            response = restClient.performRequest("GET", "/flume-copyright/_search",
                    Collections.singletonMap("pretty", "true"), entity);

            System.out.println(response.getRequestLine().getUri());

            System.out.println(EntityUtils.toString(response.getEntity()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            restClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
