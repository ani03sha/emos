package org.anirudh.redquark.emos;

import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.junit.Test;

import junit.framework.TestCase;

/**
 * Unit test for simple App.
 */
public class EmosTopologyTest extends TestCase {
	
	private String[] testString = {"Good job"};

	@Test
	public void test()
			throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, InterruptedException {
		EmosTopology.main(null);
	}
}
