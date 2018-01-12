package org.anirudh.redquark.emos.spout;

import java.util.Map;
import java.util.Random;

import org.anirudh.redquark.emos.constant.Constants;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class DataFeedSpout extends BaseRichSpout {

	/**
	 * Default generated serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * This output collector exposes the API for emitting tuples from an
	 * org.apache.storm.topology.IRichSpout
	 */
	private SpoutOutputCollector collector;

	/**
	 * Creating instance of java.util.Random class
	 */
	private Random random;

	/**
	 * Data structure to hold the input data stream
	 */
	private String[] sentences;

	/**
	 * Default method which is called when a spout is executed
	 */
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.random = new Random();

		/*
		 * Reading from the data source. Here we are just creating an array of some
		 * sentences as input data
		 */
		sentences = new String[] { "abort and abort and calm", "admire FSF admire GNU crash DRM", "nothing relevant",
				"calm when others cannot", "bonus sometimes works", "abort and crash" };
	}

	/**
	 * When this method is called, Storm is requesting that the Spout emit tuples to
	 * the output collector. This method should be non-blocking, so if the Spout has
	 * no tuples to emit, this method should return. nextTuple, ack, and fail are
	 * all called in a tight loop in a single thread in the spout task. When there
	 * are no tuples to emit, it is courteous to have nextTuple sleep for a short
	 * amount of time (like a single millisecond) so as not to waste too much CPU.
	 */
	@Override
	public void nextTuple() {

		Utils.sleep(100);
		String sentence = sentences[random.nextInt(sentences.length)];

		/* Emitting the spout */
		collector.emit(new Values(sentence));

	}

	/**
	 * Declare the output schema for all the streams of this topology.
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		/**
		 * This is used to declare output stream ids, output fields, and whether or not
		 * each output stream is a direct stream
		 */
		declarer.declare(new Fields(Constants.TUPLE_VAR_MSG));

	}

	/**
	 * Storm has determined that the tuple emitted by this spout with the msgId
	 * identifier has been fully processed. Typically, an implementation of this
	 * method will take that message off the queue and prevent it from being
	 * replayed.
	 */
	@Override
	public void ack(Object id) {
	}

	/**
	 * The tuple emitted by this spout with the msgId identifier has failed to be
	 * fully processed. Typically, an implementation of this method will put that
	 * message back on the queue to be replayed at a later time.
	 * 
	 */
	@Override
	public void fail(Object id) {
	}
}
