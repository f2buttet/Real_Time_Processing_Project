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

public class BpiParsingBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    private Double bpi_eur_rate;
    private String bpi_time;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
        bpi_eur_rate = 0.;
        bpi_time = "";
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
        bpi_eur_rate = 0.;
        bpi_time = "";
        JSONParser jsonParser = new JSONParser();
        JSONObject obj = (JSONObject)jsonParser.parse(input.getStringByField("value")); // Tuple content is in 'value'
        JSONObject obj_bpi = (JSONObject)obj.get("bpi");
        JSONObject obj_bpi_EUR = (JSONObject)obj_bpi.get("EUR");
        JSONObject obj_bpi_time = (JSONObject)obj.get("time");

        bpi_eur_rate = (Double) obj_bpi_EUR.get("rate_float");
        bpi_time = (String) obj_bpi_time.get("updatedISO");

        // Add 1 hour to bpi_time..
        CommonUtils cu = new CommonUtils();
        bpi_time = cu.addHoursToString(bpi_time, 1, "yyyy-MM-dd'T'HH:mm:00+00:00");

        // emit to bolts
        Values values = new Values(bpi_time, bpi_eur_rate);
        outputCollector.emit(values);
        outputCollector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("bpi_time", "bpi_eur_rate"));
    }

}
