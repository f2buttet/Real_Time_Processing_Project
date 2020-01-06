package btc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

public class App {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {

        final TopologyBuilder tp = new TopologyBuilder();

        /** SPOUT **/

        // BPI (/1 minute..)
        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder_bpi = KafkaSpoutConfig.builder("localhost:9092, localhost:9093, localhost:9094", "p3-btc-bpi");
        spoutConfigBuilder_bpi.setGroupId("monitor-btc-bpi");
        KafkaSpoutConfig<String, String> spoutConfig_bpi = spoutConfigBuilder_bpi.build();
        tp.setSpout("btc-spout-bpi", new KafkaSpout<String, String>(spoutConfig_bpi), 3);

        // Blocks
        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder_blocks = KafkaSpoutConfig.builder("localhost:9092, localhost:9093, localhost:9094", "p3-btc-blocks");
        spoutConfigBuilder_blocks.setGroupId("monitor-btc-blocks");
        KafkaSpoutConfig<String, String> spoutConfig_blocks = spoutConfigBuilder_blocks.build();
        tp.setSpout("btc-spout-blocks", new KafkaSpout<String, String>(spoutConfig_blocks), 3);

        // Transactions
        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder_transactions = KafkaSpoutConfig.builder("localhost:9092, localhost:9093, localhost:9094", "p3-btc-transactions");
        spoutConfigBuilder_transactions.setGroupId("monitor-btc-transactions");
        KafkaSpoutConfig<String, String> spoutConfig_transactions = spoutConfigBuilder_transactions.build();
        tp.setSpout("btc-spout-transactions", new KafkaSpout<String, String>(spoutConfig_transactions), 3);


        /** BOLT - PARSING **/

        tp.setBolt("raw-parsing", new RawParsingBolt(), 3)
                .shuffleGrouping("btc-spout-bpi")
                .shuffleGrouping("btc-spout-blocks")
                .shuffleGrouping("btc-spout-transactions");

        tp.setBolt("data-parsing-bpi", new BpiParsingBolt(), 3)
                .shuffleGrouping("btc-spout-bpi");

        tp.setBolt("data-parsing-blocks", new BlocksParsingBolt(), 3)
                .shuffleGrouping("btc-spout-blocks");

        tp.setBolt("data-parsing-transactions", new TransactionParsingBolt(), 3)
                .shuffleGrouping("btc-spout-transactions");

        /** BOLT - INJECT **/

        // inject bpi for 'transactions processing bolts' every minute
        tp.setBolt("exchange-transactions-bpi", new BtcTransactionsBpiBolt().withTumblingWindow(BaseWindowedBolt.Duration.of(1000*60*1)), 3)
                .allGrouping("data-parsing-bpi")
                .fieldsGrouping("data-parsing-transactions", new Fields("transactions_time"));

        // inject bpi for 'blocks processing bolts'every minute
        tp.setBolt("miner-blocks-bpi", new BtcBlocksBpiBolt().withTumblingWindow(BaseWindowedBolt.Duration.of(1000*60*1)),3)
                .allGrouping("data-parsing-bpi")
                .fieldsGrouping("data-parsing-blocks", new Fields("blocks_foundBy"));


        /** BOLT - PROCESSING **/

        // Calcul transactions volume btc + eur
        tp.setBolt("exchange-transactions", new BtcVolumeExchangeBolt(), 3)
                .fieldsGrouping("exchange-transactions-bpi", new Fields("transactions_time"));


        // Calcul miner value btc + eur
        tp.setBolt("miner-blocks", new BtcBestMinerBolt(),3)
                .fieldsGrouping("miner-blocks-bpi", new Fields("blocks_foundBy"));


        /** BOLT - SAVE **/

        tp.setBolt("save-rawData", new SaveRawDataBolt(), 3)
                .shuffleGrouping("raw-parsing");

        tp.setBolt("save-bpi", new SaveBpiBolt(), 3)
                .shuffleGrouping("data-parsing-bpi");

        tp.setBolt("save-exchange-transactions", new SaveVolumeExchangeBolt(), 3)
                .fieldsGrouping("exchange-transactions", new Fields("transactions_time"));

        tp.setBolt("save-miner-blocks", new SaveBestMinerBolt(), 3)
                .fieldsGrouping("miner-blocks", new Fields("blocks_foundBy"));

        /** TOPOLOGY **/

        StormTopology topology = tp.createTopology();

        Config config = new Config();
        config.setMessageTimeoutSecs(120); // because of Window duration (length + sliding interval) value 120000
        String topologyName = "btc";
        if (args.length > 0 && args[0].equals("remote")) {
            StormSubmitter.submitTopology(topologyName, config, topology);
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, config, topology);
        }
    }

}
