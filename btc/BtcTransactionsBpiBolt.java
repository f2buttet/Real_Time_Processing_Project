package btc;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.Map;

public class BtcTransactionsBpiBolt extends BaseWindowedBolt {
    private OutputCollector outputCollector;
    private Long transactions_timestamp;
    private Double transactions_total_amount;
    private String transactions_time;
    private Double bpi_eur_rate;
    private String bpi_time;
    private HashMap<String, Double> bpi_values = new HashMap<String, Double>();
    private Double last_bpi_eur_rate;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
        transactions_timestamp = 0l;
        transactions_total_amount = 0.;
        transactions_time = "";
        bpi_eur_rate = 0.;
        bpi_time = "";
        last_bpi_eur_rate = 0.;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        bpi_eur_rate = 0.;
        bpi_time = "";
        transactions_timestamp = 0l;
        transactions_total_amount = 0.;
        transactions_time = "";
        last_bpi_eur_rate = 0.;

        for (Tuple input: tupleWindow.get()) {
            if (input.getSourceComponent().equals("data-parsing-bpi")) {
                bpi_eur_rate = input.getDoubleByField("bpi_eur_rate");
                bpi_time = input.getStringByField("bpi_time");
                bpi_values.put(bpi_time, bpi_eur_rate);
                outputCollector.ack(input);
            }
        }
        last_bpi_eur_rate = bpi_eur_rate;
        for (Tuple input: tupleWindow.get()) {
            if (input.getSourceComponent().equals("data-parsing-transactions")) {
                transactions_timestamp = input.getLongByField("transactions_timestamp");
                transactions_total_amount = input.getDoubleByField("transactions_total_amount");
                transactions_time = input.getStringByField("transactions_time");

                outputCollector.ack(input);
                Values values = new Values(transactions_timestamp, transactions_time, transactions_total_amount, (bpi_values.get(transactions_time) == null ? last_bpi_eur_rate : bpi_values.get(transactions_time) ));
                outputCollector.emit(values);
            }
            else {
                outputCollector.fail(input);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("transactions_timestamp", "transactions_time", "transactions_total_amount", "bpi_eur_rate"));
    }
}
