package btc;

import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class RawParsingBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    private JSONObject obj;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            process(input);
        } catch (ParseException e) {
            e.printStackTrace();
            outputCollector.fail(input);
        }
    }

    public void process(Tuple input) throws ParseException {
        String type = "";
        JSONParser jsonParser = new JSONParser();
        JSONObject obj = (JSONObject)jsonParser.parse(input.getStringByField("value"));

        if (input.getSourceComponent().equals("btc-spout-bpi")) {
            type = "bpi";
        }
        if (input.getSourceComponent().equals("btc-spout-blocks")) {
            type = "blocks";
        }
        if (input.getSourceComponent().equals("btc-spout-transactions")) {
            type = "transactions";
        }
        Values values = new Values(type,String.valueOf(obj));
        outputCollector.emit(values);
        outputCollector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("type","raw"));
    }

}