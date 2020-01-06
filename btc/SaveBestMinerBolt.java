package btc;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.Map;

public class SaveBestMinerBolt extends BaseRichBolt {

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

        String blocks_time = (String) input.getStringByField("blocks_time");
        String blocks_foundBy = (String) input.getStringByField("blocks_foundBy");
        Double block_reward_btc = (Double) input.getDoubleByField("block_reward_btc");
        Double block_reward_eur = (Double) input.getDoubleByField("block_reward_eur");
        String format = "{\"blocks_time\":\"" + blocks_time
                + "\", \"blocks_foundBy\":\"" + blocks_foundBy
                + "\", \"block_reward_btc\":" + block_reward_btc
                + ", \"block_reward_eur\":" + block_reward_eur
                + "}";
        esApi = new ElasticSearch();
        esApi.indexApi("btc-miner", "best-miner", format);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
