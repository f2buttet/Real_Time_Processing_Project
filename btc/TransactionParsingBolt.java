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

public class TransactionParsingBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private Long transactions_timestamp;
    private Double transactions_total_amount;
    private String transactions_time;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
        transactions_timestamp = 0l;
        transactions_total_amount = 0.;
        transactions_time = "";
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
        transactions_timestamp = (Long) obj.get("timestamp");
        transactions_total_amount = (Double) obj.get("total_amount");

        transactions_time = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:00+00:00").format(new Date((long)transactions_timestamp*1000));
        CommonUtils cu = new CommonUtils();
        transactions_time = cu.addHoursToString(transactions_time, 1, "yyyy-MM-dd'T'HH:mm:00+00:00");

        Values values = new Values(transactions_timestamp, transactions_time, transactions_total_amount);
        outputCollector.emit(values);
        outputCollector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("transactions_timestamp", "transactions_time", "transactions_total_amount"));
    }

}
