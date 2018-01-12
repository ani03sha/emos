package org.anirudh.redquark.emos.bolt;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Simple Bolt that check words of incoming sentence and mark sentence with a
 * positive score.
 * 
 * @author anirshar
 *
 */
public class PositiveBolt extends BaseBasicBolt {

	/**
	 * Default generated serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * SLF4J logger
	 */
	private static final Logger LOG = LoggerFactory.getLogger(NegativeBolt.class);

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
		mapper.setSerializationInclusion(Include.NON_NULL);
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		try {
			/* Reading the first element from the Tuple instance */
			ObjectNode node = (ObjectNode) mapper.readTree(tuple.getString(0));

			/* Array of words */
			String[] words = node.path(Constants.MOD_TXT).asText().split(" ");

			/* Getting the length of an array */
			int wordsSize = words.length;

			/* Size of positive words */
			int positiveWordsSize = 0;

			for (String word : words) {
				/* Check if the current word is a positive word */
				if (PositiveWords.get().contains(word)) {
					/* If the current word is a positive word, incrementing the size by one */
					positiveWordsSize++;
				}
			}

			node.put(Constants.NUM_POSITIVE, (double) positiveWordsSize / wordsSize);

			/* Emitting the node */
			collector.emit(new Values(node.toString()));

		} catch (Exception e) {
			LOG.error("Cannot process input. Ignore it", e);
		}

	}

	/**
	 * Declare the output schema for all the streams of this topology.
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields(Constants.TUPLE_VAR_MSG));
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

	/**
	 * Singleton class for giving the unique instance of PositiveWords
	 * 
	 * @author anirshar
	 *
	 */
	private static class PositiveWords {

		/*
		 * Set of positive words. Set, because, we do not want positive words to be
		 * repeated
		 */
		private Set<String> positiveWords;

		/* Single object of this singleton class */
		private static PositiveWords singleton;

		/**
		 * Private constructor so that no other instance can be generated from outside
		 * this class
		 */
		private PositiveWords() {

			/* Initializing the set of positive words */
			positiveWords = new HashSet<>();

			/* Adding more positive words or load from a file or database */
			positiveWords.add("admire");
			positiveWords.add("bonus");
			positiveWords.add("calm");
			positiveWords.add("good");
			positiveWords.add("accept");
			positiveWords.add("happiness");
			positiveWords.add("amazing");
		}

		/**
		 * Static get method responsible for providing the unique instance of positive
		 * words
		 * 
		 * @return PositiveWords
		 */
		static PositiveWords get() {

			if (singleton == null) {
				synchronized (PositiveWords.class) {
					/* Double lock checking for multi-threaded environment */
					if (singleton == null) {
						singleton = new PositiveWords();
					}
				}
			}
			return singleton;
		}

		/**
		 * Method to check if the passed word contains a positive word
		 * 
		 * @param key
		 * @return boolean
		 */
		boolean contains(String key) {
			return get().positiveWords.contains(key);
		}
	}

}
