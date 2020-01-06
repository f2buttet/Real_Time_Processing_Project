package btc;

import org.apache.storm.shade.org.json.simple.parser.ParseException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class BtcBestMinerBolt extends BaseRichBolt {
    private OutputCollector outputCollector;

    private HashMap<String, Integer> miner_count;
    private HashMap<String, HashMap<String, Integer>> miner_data;

    private String blocks_foundBy;
    private Double average_value_eur = 0.;
    private Double btcVal = 12.5;
    private String blocks_time;
    private String timing_day;
    private String testStr = "OK";
    private Integer testInt = 1;
    private Boolean isInTimeSlot;


    private HashMap<String, Integer> miner_count_full = new HashMap<String, Integer>();
    private HashMap<String, Integer> miner_count_2 = new HashMap<String, Integer>();
    private HashMap<String, Double> average_values = new HashMap<String, Double>();
    private HashMap<String, HashMap<String, Integer>> miner_data_full = new HashMap<String, HashMap<String, Integer>>();
    private String x = "";
    private String y = "";
    private ArrayList<String> time_values = new ArrayList<String>();
    private Integer loopRaz = 0;
    private Double bpi_eur_rate = 0.;
    private HashMap<String, Double> hm_eur_value = new HashMap<String, Double>();
private String format_obj = "";
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
        miner_count = new HashMap<String, Integer>();
        miner_data = new HashMap<String, HashMap<String, Integer>>();
        blocks_foundBy = "";
        timing_day = "";
        blocks_time = "";
        bpi_eur_rate = 0.;

    }

    @Override
    public void execute(Tuple input) {
        try {
            process(input);
            outputCollector.ack(input);
        } catch (ParseException e) {
            e.printStackTrace();
            outputCollector.fail(input);
        }
    }

    public void process(Tuple input) throws ParseException {
        blocks_foundBy = input.getStringByField("blocks_foundBy");
        blocks_time = input.getStringByField("blocks_time");
        bpi_eur_rate = input.getDoubleByField("bpi_eur_rate");

        Double block_reward_btc = btcVal;
        Double block_reward_eur = btcVal * bpi_eur_rate;

        Values values = new Values(blocks_time, blocks_foundBy, block_reward_btc, block_reward_eur);
        outputCollector.emit(values);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("blocks_time", "blocks_foundBy", "block_reward_btc", "block_reward_eur"));
    }
}
