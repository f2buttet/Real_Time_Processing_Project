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

public class BtcBlocksBpiBolt extends BaseWindowedBolt {
    private OutputCollector outputCollector;
    private String blocks_foundBy;
    private Double bpi_eur_rate;
    private String blocks_time;

    private String bpi_time = "";
    private HashMap<String, Double> bpi_values = new HashMap<String, Double>();
    private Double last_bpi_eur_rate = 0.;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
        blocks_foundBy = "";
        blocks_time = "";
        bpi_eur_rate = 0.;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        bpi_eur_rate = 0.;
        bpi_time = "";
        blocks_foundBy = "";
        blocks_time = "";

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
            if (input.getSourceComponent().equals("data-parsing-blocks")) {
                blocks_foundBy = input.getStringByField("blocks_foundBy");
                blocks_time = input.getStringByField("blocks_time");

                outputCollector.ack(input);
                Values values = new Values(blocks_time, blocks_foundBy, (bpi_values.get(blocks_time) == null ? last_bpi_eur_rate : bpi_values.get(blocks_time)));
                outputCollector.emit(values);
            }
            else {
                outputCollector.fail(input);
            }

        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("blocks_time", "blocks_foundBy", "bpi_eur_rate" ));
    }
}

