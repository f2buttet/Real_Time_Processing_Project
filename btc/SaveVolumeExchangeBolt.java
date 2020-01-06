package btc;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.Map;

public class SaveVolumeExchangeBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    private ElasticSearch esApi;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            process(input);
            outputCollector.ack(input);
        } catch (IOException e) {
            e.printStackTrace();
            outputCollector.fail(input);
        }
    }

    public void process(Tuple input) throws IOException {
        String transactions_time = (String) input.getStringByField("transactions_time");
        Double volume_exchange = (Double) input.getDoubleByField("volume_exchange");
        Double volume_exchange_eur = (Double) input.getDoubleByField("volume_exchange_eur");

        // Go To ELASTICSEARCH
        String format = "{\"transactions_time\":\"" + transactions_time +
                "\", \"volume_exchange\":" + volume_exchange +
                ", \"volume_exchange_eur\":" + volume_exchange_eur +
                "}";
        esApi = new ElasticSearch();
        esApi.indexApi("btc-volume", "volume", format);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}