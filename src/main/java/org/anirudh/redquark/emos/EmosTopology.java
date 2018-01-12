package org.anirudh.redquark.emos;

import org.anirudh.redquark.emos.bolt.LoggingBolt;
import org.anirudh.redquark.emos.bolt.NegativeBolt;
import org.anirudh.redquark.emos.bolt.PositiveBolt;
import org.anirudh.redquark.emos.bolt.ScoreBolt;
import org.anirudh.redquark.emos.bolt.StemmingBolt;
import org.anirudh.redquark.emos.constant.Constants;
import org.anirudh.redquark.emos.spout.DataFeedSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Simple topology class that defines the process flow.
 * 
 * @author anirshar
 *
 */
public class EmosTopology {
	public static void main(String[] args)
			throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, InterruptedException {

		/* Creating an instance of this topology */
		TopologyBuilder builder = new TopologyBuilder();

		/* Setting the spout */
		builder.setSpout("spout", new DataFeedSpout());

		/* Setting the bolts */
		builder.setBolt("stemming", new StemmingBolt(), 1).localOrShuffleGrouping("spout");
		builder.setBolt("positive", new PositiveBolt(), 1).localOrShuffleGrouping("stemming");
		builder.setBolt("negative", new NegativeBolt(), 1).localOrShuffleGrouping("positive");
		builder.setBolt("score", new ScoreBolt(), 1).localOrShuffleGrouping("negative");
		builder.setBolt("logging", new LoggingBolt().withFields(Constants.TUPLE_VAR_MSG, Constants.TUPLE_VAR_SCORE), 1)
				.localOrShuffleGrouping("score");

		/* Creating a configuration for this toplogy */
		Config config = new Config();
		config.setDebug(true);

		if (args != null && args.length > 0) {
			/* Setting the number of worker threads for this config */
			config.setNumWorkers(1);

			/* Submitting this toplogy */
			StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
		}else {
			
			/* Creating a local cluster*/
			LocalCluster cluster = new LocalCluster();
			
			/* Submitting the topology */
			cluster.submitTopology("emos_toplogy", config, builder.createTopology());
			
			Thread.sleep(10000);
			
			/* Shutting down the cluster */
			cluster.shutdown();
		}
	}
}
