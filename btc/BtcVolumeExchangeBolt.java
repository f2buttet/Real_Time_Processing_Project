package btc;

import org.apache.storm.shade.org.json.simple.parser.ParseException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class BtcVolumeExchangeBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private Long transactions_timestamp;
    private Double transactions_total_amount;
    private Double volume_exchange;
    private Double bpi_eur_rate;
    private String transactions_time;
    private Double volume_exchange_eur;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
        transactions_timestamp = 0l;
        transactions_total_amount = 0.;
        volume_exchange = 0.;
        bpi_eur_rate = 0.;
        transactions_time ="";
        volume_exchange_eur = 0.;
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
        bpi_eur_rate =0.;
        volume_exchange = 0.;
        transactions_timestamp = 0l;
        transactions_time = "";
        transactions_total_amount = 0.;
        volume_exchange_eur = 0.;

        transactions_timestamp = input.getLongByField("transactions_timestamp");
        transactions_time = input.getStringByField("transactions_time");
        transactions_total_amount = input.getDoubleByField("transactions_total_amount");
        bpi_eur_rate = input.getDoubleByField("bpi_eur_rate");

        volume_exchange = transactions_total_amount;
        volume_exchange_eur = transactions_total_amount * bpi_eur_rate;

            Values values = new Values(transactions_time, volume_exchange, volume_exchange_eur);
            outputCollector.emit(values);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("transactions_time", "volume_exchange", "volume_exchange_eur"));
    }
}
