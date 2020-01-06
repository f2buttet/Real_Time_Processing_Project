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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class BlocksParsingBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private String blocks_time;
    private String blocks_foundBy;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
        blocks_time = "";
        blocks_foundBy = "";
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
        JSONParser jsonParser = new JSONParser();
        JSONObject obj = (JSONObject)jsonParser.parse(input.getStringByField("value"));

        blocks_time = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:00+00:00").format(new Date((long)obj.get("timestamp")*1000));
        CommonUtils cu = new CommonUtils();
        blocks_time = cu.addHoursToString(blocks_time, 1, "yyyy-MM-dd'T'HH:mm:00+00:00");

        blocks_foundBy = (String) obj.get("foundBy");

        Values values = new Values(blocks_time, blocks_foundBy);
        outputCollector.emit(values);
        outputCollector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("blocks_time", "blocks_foundBy"));
    }


}
