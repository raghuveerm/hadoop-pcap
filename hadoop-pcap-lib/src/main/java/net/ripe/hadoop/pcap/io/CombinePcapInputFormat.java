package net.ripe.hadoop.pcap.io;

import java.io.IOException;

import net.ripe.hadoop.pcap.io.reader.CombinePcapRecordReader;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class CombinePcapInputFormat extends CombineFileInputFormat<LongWritable, ObjectWritable> {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public RecordReader<LongWritable, ObjectWritable> createRecordReader(InputSplit split,
			TaskAttemptContext job) throws IOException {

		return new CombineFileRecordReader((CombineFileSplit) split, job,
				CombinePcapRecordReader.class);
	}

	/**
	 * A PCAP can only be read as a whole. There is no way to know where to
	 * start reading in the middle of the file. It needs to be read from the
	 * beginning to the end.
	 * @see http://wiki.wireshark.org/Development/LibpcapFileFormat
	 */
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return Boolean.FALSE;
	}

}
