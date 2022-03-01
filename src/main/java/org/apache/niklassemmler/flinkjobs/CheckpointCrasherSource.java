package org.apache.niklassemmler.flinkjobs;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;


public final class CheckpointCrasherSource
        extends RichSourceFunction<Integer> implements CheckpointListener {

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        while (running) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(getRuntimeContext().getIndexOfThisSubtask());
                Thread.sleep(5L);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        throw new RuntimeException("Test exception.");
    }
}
