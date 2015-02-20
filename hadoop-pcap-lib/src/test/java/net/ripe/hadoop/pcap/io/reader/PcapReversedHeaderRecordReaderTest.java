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
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PcapReversedHeaderRecordReaderTest {

	private static final Log LOG = LogFactory.getLog(PcapReversedHeaderRecordReaderTest.class);

	private final File TEST_FILE = new File("src/test/resources/reversedtest.pcap");

	private PcapRecordReader recordReader;

	@Test
	public void progress(){
		try {
			assertTrue(PcapReader.HEADER_SIZE / (float) TEST_FILE.length() == recordReader
					.getProgress());
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
			while (recordReader.nextKeyValue())
				;
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
	}

	@Before
	public void startup() throws IOException {
		JobConf config = new JobConf();
		FileSystem fs = FileSystem.get(config);
		FSDataInputStream is = fs.open(new Path(TEST_FILE.getParent(), TEST_FILE.getName()));
		recordReader = new PcapRecordReader(new PcapReader(is), 0L, TEST_FILE.length(), is, is);
	}

	@After
	public void shutdown() throws IOException {
		recordReader.stream.close();
	}

	private class TestableReporter implements Reporter {
		@Override
		public void setStatus(String status) {
			LOG.debug(status);
		}

		@Override
		public Counter getCounter(Enum<?> name) {
			return null;
		}

		@Override
		public Counter getCounter(String group, String name) {
			return null;
		}

		@Override
		public InputSplit getInputSplit() throws UnsupportedOperationException {
			return null;
		}

		@Override
		public void incrCounter(Enum<?> key, long amount) {
		}

		@Override
		public void incrCounter(String group, String counter, long amount) {
		}

		@Override
		public void progress() {
		}

		@Override
		public float getProgress() {
			return 0;
		}
	}
}