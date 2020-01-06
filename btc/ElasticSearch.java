package btc;


import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

public class ElasticSearch {


    public RestHighLevelClient client;


    public ElasticSearch() {

        try {
            client = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("localhost", 9200, "http")
                    )
            );

        } catch (Exception e) {
            System.out.println("catch - Elastic constructor");
            System.out.println(e);
        }
    }

    public void indexApi(String index
                        , String key
                        , String val) {

        try {
            IndexRequest request = new IndexRequest(index);

            String jsonSource = "{\"" + key + "\":" + val + "}";

            request.source(jsonSource, XContentType.JSON);

            IndexResponse response = client.index(request, RequestOptions.DEFAULT);

            RestStatus status = response.status();
            client.close();
        } catch (Exception e) {
            System.out.println("catch - Elastic indexApi");
            System.out.println(e);

        }

    }
}
