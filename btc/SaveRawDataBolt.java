package btc;

import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.Map;

public class SaveRawDataBolt extends BaseRichBolt {

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
        } catch (ParseException e) {
            e.printStackTrace();
            outputCollector.fail(input);
        }
    }

    public void process(Tuple input) throws ParseException {
        JSONObject raw_data = (JSONObject) new JSONParser().parse(input.getStringByField("raw"));
        String type = input.getStringByField("type");

        try {

            // Go To ELASTICSEARCH
            Calendar calendrier = Calendar.getInstance(new Locale("fr", "FR"));
            String format = "{\"raw_time\":\"" + new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss+00:00").format(calendrier.getTime()) + "\", \"raw-data\":" + raw_data + "}";
            esApi = new ElasticSearch();
            switch (type) {
                case "bpi":
                    esApi.indexApi("btc-raw-bpi", "raw", format);
                    break;
                case "blocks":
                    esApi.indexApi("btc-raw-blocks", "raw", format);
                    break;
                case "transactions":
                    esApi.indexApi("btc-raw-transactions", "raw", format);
                    break;
            }

        } catch (Exception e) {
            System.out.println("catch PARSER");
            System.out.println(e);
            e.printStackTrace();
            outputCollector.fail(input);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}