package net.ripe.hadoop.pcap.io.reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import net.ripe.hadoop.pcap.PcapReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PcapIPv6RecordReaderTest {
	
	private static final Log LOG = LogFactory.getLog(PcapIPv6RecordReaderTest.class);

	private final File TEST_FILE = new File("src/test/resources/test-ipv6.pcap");

	private PcapRecordReader recordReader;

	@Test
	public void progress() {
		
		try {
			assertTrue(PcapReader.HEADER_SIZE / (float)TEST_FILE.length() == recordReader.getProgress());
			skipToEnd();
			assertTrue(1.0 == recordReader.getProgress());
		} catch (InterruptedException  e) {
			LOG.error("Invalid values");
			e.printStackTrace();
		} catch (IOException e) {
			LOG.error("unable to do IO operations");
		}
		
	}

	@Test
	public void position() throws IOException {
		assertEquals(PcapReader.HEADER_SIZE, recordReader.getPos());
		skipToEnd();
		assertEquals(TEST_FILE.length(), recordReader.getPos());
	}

	private void skipToEnd() throws IOException {
		try {
			while (recordReader.nextKeyValue());
		} catch (InterruptedException e) {
			
			e.printStackTrace();
		}		
	}

	@Before
	public void startup() throws IOException {
		Job config = new Job();
		FileSystem fs = FileSystem.get(config.getConfiguration());
		FSDataInputStream is = fs.open(new Path(TEST_FILE.getParent(), TEST_FILE.getName()));
		recordReader = new PcapRecordReader(new PcapReader(is), 0L, TEST_FILE.length(), is, is);
	}

	@After
	public void shutdown() throws IOException {
		recordReader.stream.close();
	}

}