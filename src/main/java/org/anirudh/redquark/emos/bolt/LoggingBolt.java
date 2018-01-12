package org.anirudh.redquark.emos.bolt;

import java.util.Map;

import org.anirudh.redquark.emos.constant.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic terminal Bolt that just logs input fields.
 * 
 * @author anirshar
 *
 */
public class LoggingBolt extends BaseRichBolt {

	/**
	 * Default generated serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * SLF4J logger
	 */
	private static final Logger LOG = LoggerFactory.getLogger(ScoreBolt.class);

	/**
	 * Creating an instance of output collector
	 */
	private OutputCollector collector;

	/**
	 * Creating a flag for the error
	 */
	private boolean error = false;

	/**
	 * Creating a string array which contains the fields
	 */
	private String[] fields;

	/**
	 * Called when a task for this component is initialized within a worker on the
	 * cluster. It provides the bolt with the environment in which the bolt
	 * executes.
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

		/* Initializing the collector */
		this.collector = collector;

		/**
		 * Checking if the "fields array is null"
		 */
		if (fields == null) {
			/* Initializing the array if it is null */
			fields = new String[] { Constants.TUPLE_VAR_MSG };
		}
	}

	/**
	 * Processes a single tuple of input. The Tuple object contains metadata on it
	 * about which component/stream/task it came from. The values of the Tuple can
	 * be accessed using Tuple#getValue. The IBolt does not have to process the
	 * Tuple immediately. It is perfectly fine to hang onto a tuple and process it
	 * later (for instance, to do an aggregation or join). Tuples should be emitted
	 * using the OutputCollector provided through the prepare method. It is required
	 * that all input tuples are acked or failed at some point using the
	 * OutputCollector. Otherwise, Storm will be unable to determine when tuples
	 * coming off the spouts have been completed.
	 */
	@Override
	public void execute(Tuple tuple) {

		if (error) {
			for (String field : fields) {
				LOG.error("{}: {}", field, tuple.getValueByField(field));
			}
		}

		/* Acknowledging the tuple */
		collector.ack(tuple);
	}

	/**
	 * Declare the output schema for all the streams of this topology.
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	public LoggingBolt withFields(String... fieldNames) {
		this.fields = fieldNames;
		return this;
	}

	/**
	 * Called in case error occurs
	 * 
	 * @param errorCase
	 * @return
	 */
	public LoggingBolt withError(boolean errorCase) {
		this.error = errorCase;
		return this;
	}
}
