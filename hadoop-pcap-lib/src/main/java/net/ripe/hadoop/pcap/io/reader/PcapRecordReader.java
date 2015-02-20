package net.ripe.hadoop.pcap.io.reader;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;

import net.ripe.hadoop.pcap.PcapReader;
import net.ripe.hadoop.pcap.packet.Packet;

import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class PcapRecordReader extends RecordReader<LongWritable, ObjectWritable> {

	PcapReader pcapReader;
	Iterator<Packet> pcapReaderIterator;
	Seekable baseStream;
	DataInputStream stream;

	long packetCount = 0;
	long start, end;

	private LongWritable key = new LongWritable();

	private ObjectWritable value = new ObjectWritable();

	public PcapRecordReader(PcapReader pcapReader, long start, long end, Seekable baseStream,
			DataInputStream stream) throws IOException {
		this.pcapReader = pcapReader;
		this.baseStream = baseStream;
		this.stream = stream;
		this.start = start;
		this.end = end;

		pcapReaderIterator = pcapReader.iterator();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		if (!pcapReaderIterator.hasNext())
			return false;

		key.set(++packetCount);
		value.set(pcapReaderIterator.next());

		return true;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {

		return key;
	}

	@Override
	public ObjectWritable getCurrentValue() throws IOException, InterruptedException {

		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {

		if (start == end)
			return 0;
		return Math.min(1.0f, (getPos() - start) / (float) (end - start));
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {

	}

	public long getPos() throws IOException {
		return baseStream.getPos();
	}

	@Override
	public void close() throws IOException {
		stream.close();
	}

}