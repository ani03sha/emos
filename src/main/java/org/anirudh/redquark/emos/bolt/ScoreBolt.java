package org.anirudh.redquark.emos.bolt;

import java.util.Map;

import org.anirudh.redquark.emos.constant.Constants;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import clojure.lang.Cons;

/**
 * Simple Bolt that check incoming positive and negative scores and decides if
 * this sentence is positive or negative.
 * 
 * @author anirshar
 *
 */
public class ScoreBolt extends BaseBasicBolt {

	/**
	 * Default generated serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * SLF4J logger
	 */
	private static final Logger LOG = LoggerFactory.getLogger(ScoreBolt.class);

	/**
	 * Creating instance of ObjectMapper
	 */
	private transient ObjectMapper mapper;

	/**
	 * First method which gets called while executing a bolt
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		/* Initializing the mapper */
		mapper = new ObjectMapper();
	}

	/**
	 * Process the input tuple and optionally emit new tuples based on the input
	 * tuple.
	 */
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		try {
			/* Reading the first element from the Tuple instance */
			ObjectNode node = (ObjectNode) mapper.readTree(tuple.getString(0));

			boolean score = false;

			/* Checking which score is higher positive or negative */
			if (node.get(Constants.NUM_NEGATIVE).asDouble(Double.NEGATIVE_INFINITY) > node.get(Constants.NUM_POSITIVE)
					.asDouble(Double.POSITIVE_INFINITY)) {

				node.put(Constants.SCORE, Constants.SENTIMENT_NEG);
				System.out.println("this is negative");
				score = false;
			} else {
				node.put(Constants.SCORE, Constants.SENTIMENT_POS);
				System.out.println("this is positive");
				score = true;
			}

			/* Emitting the new data */
			collector.emit(new Values(node.path(Constants.ID).asText(), node.toString(), score));
		} catch (Exception e) {
			LOG.error("Cannot process the input. Ignore it!", e);
		}

	}

	/**
	 * Declare the output schema for all the streams of this topology.
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(Constants.TUPLE_VAR_KEY, Constants.TUPLE_VAR_MSG, Constants.TUPLE_VAR_SCORE));
	}

	/**
	 * Declare configuration specific to this component. Only a subset of the
	 * "topology.*" configurations can be overridden. The component configuration
	 * can be further overridden when constructing the topology using
	 * TopologyBuilder
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
