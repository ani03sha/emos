package org.anirudh.redquark.emos.bolt;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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
 * Simple Bolt that checks incoming sentence and remove any words that are
 * useless for scoring by next processing Bolts.
 * 
 * @author anirshar
 *
 */
public class StemmingBolt extends BaseBasicBolt {

	/**
	 * Default generated serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * SLF4J logger
	 */
	private static final Logger LOG = LoggerFactory.getLogger(StemmingBolt.class);

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

	/**
	 * Process the input tuple and optionally emit new tuples based on the input
	 * tuple.
	 */
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		try {
			String text = tuple.getString(0);
			String[] words = text.split("\\b");
			StringBuilder modified = new StringBuilder(text.length() / 2);

			for (String w : words) {
				if (!UselessWords.get().contains(w)) {
					modified.append(w);
				}
			}

			ObjectNode node = mapper.createObjectNode();

			/* Generate a business id or get it as input */
			node.put(Constants.ID, UUID.randomUUID().toString());
			node.put(Constants.TEXT, text);
			node.put(Constants.MOD_TXT, modified.toString());

			/* Emitting the tuple */
			collector.emit(new Values(node.toString()));
			
		} catch (Exception e) {
			LOG.error("Cannot process input data! Ignore it", e);
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
	 * A singleton class to find out the useless words in a data stream
	 * 
	 * @author anirshar
	 *
	 */
	private static class UselessWords {

		/*
		 * A set of useless words. Set, because, we don't want any useless word to
		 * repeat
		 */
		private Set<String> uselessWords;

		/* Creating an instance of the singleton class */
		private static UselessWords singleton;

		/**
		 * private constructor
		 */
		private UselessWords() {
			uselessWords = new HashSet<>();

			/* Add more useless words or feed from a file or database */
			uselessWords.add("add");
			uselessWords.add("about");
			uselessWords.add("be");
			uselessWords.add("before");
		}

		/* Get method to get the singleton instance of this class */
		static UselessWords get() {
			if (singleton == null) {
				synchronized (UselessWords.class) {
					/* Double checked locking for the multi-threaded environment */
					if (singleton == null) {
						singleton = new UselessWords();
					}
				}
			}
			return singleton;
		}

		boolean contains(String key) {
			return get().uselessWords.contains(key);
		}
	}
}
