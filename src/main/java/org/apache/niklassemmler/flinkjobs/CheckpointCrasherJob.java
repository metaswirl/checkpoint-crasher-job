package org.apache.niklassemmler.flinkjobs;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

public class CheckpointCrasherJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// add source that throws an exception for each checkpoint
		DataStreamSource<Integer> src = env.addSource(new CheckpointCrasherSource());
		src.addSink(new DiscardingSink<>());

		// enable checkpoints every 15 seconds
		env.enableCheckpointing(15000);

		// execute program
		env.execute("Run test job");
	}

}
