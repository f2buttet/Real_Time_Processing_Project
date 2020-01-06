package btc;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.Map;

public class SaveBpiBolt extends BaseRichBolt {

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
        String bpi_time = (String) input.getStringByField("bpi_time");
        Double bpi_eur_rate = (Double) input.getDoubleByField("bpi_eur_rate");

        // Go To ELASTICSEARCH
        String format = "{\"bpi_time\":\"" + bpi_time + "\", \"bpi_eur_rate\":" + bpi_eur_rate + "}";
        esApi = new ElasticSearch();
        esApi.indexApi("btc-value", "bpi", format);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
