package net.ripe.hadoop.pcap.io.reader;

import java.io.IOException;

import net.ripe.hadoop.pcap.io.PcapInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class CombinePcapRecordReader extends RecordReader<LongWritable, ObjectWritable> {
	
	private PcapRecordReader recordReader;

	public CombinePcapRecordReader(CombineFileSplit split, Configuration conf, Integer index) throws IOException {
		Path path = split.getPath(index);
		long start = 0L;
		long length = split.getLength(index);
		recordReader = PcapInputFormat.initPcapRecordReader(path, start, length, conf);
	}
	
	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		
		recordReader.initialize(arg0, arg1);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		return recordReader.nextKeyValue();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		
		return recordReader.getCurrentKey();
	}

	@Override
	public ObjectWritable getCurrentValue() throws IOException, InterruptedException {
		
		return recordReader.getCurrentValue();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {

		return recordReader.getProgress();
	}

	
	@Override
	public void close() throws IOException {
		
		recordReader.close();
	}

}