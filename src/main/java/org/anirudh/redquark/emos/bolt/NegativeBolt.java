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
 * negative score. 
 * 
 * @author anirshar
 *
 */
public class NegativeBolt extends BaseBasicBolt {

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

			/* Size of negative words */
			int negativeWordsSize = 0;

			for (String word : words) {
				/* Check if the current word is a positive word */
				if (NegativeWords.get().contains(word)) {
					/* If the current word is a negative word, incrementing the size by one */
					negativeWordsSize++;
				}
			}

			node.put(Constants.NUM_NEGATIVE, (double) negativeWordsSize / wordsSize);

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
	private static class NegativeWords {

		/*
		 * Set of positive words. Set, because, we do not want positive words to be
		 * repeated
		 */
		private Set<String> negativeWords;

		/* Single object of this singleton class */
		private static NegativeWords singleton;

		/**
		 * Private constructor so that no other instance can be generated from outside
		 * this class
		 */
		private NegativeWords() {

			/* Initializing the set of positive words */
			negativeWords = new HashSet<>();

			// Add more "negative" words and load from file or database
			negativeWords.add("abort");
			negativeWords.add("betray");
			negativeWords.add("crash");
			negativeWords.add("thief");
			negativeWords.add("disappointment");
			negativeWords.add("disease");
			negativeWords.add("bad");
			negativeWords.add("sad");
			negativeWords.add("evil");
		}

		/**
		 * Static get method responsible for providing the unique instance of negative
		 * words
		 * 
		 * @return PositiveWords
		 */
		static NegativeWords get() {

			if (singleton == null) {
				synchronized (NegativeWords.class) {
					/* Double lock checking for multi-threaded environment */
					if (singleton == null) {
						singleton = new NegativeWords();
					}
				}
			}
			return singleton;
		}

		/**
		 * Method to check if the passed word contains a negative word
		 * 
		 * @param key
		 * @return boolean
		 */
		boolean contains(String key) {
			return get().negativeWords.contains(key);
		}
	}
}
