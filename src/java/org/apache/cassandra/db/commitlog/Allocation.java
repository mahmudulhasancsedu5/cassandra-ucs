package org.apache.cassandra.db.commitlog;

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.WaitQueue;

/**
 * A simple class for tracking information about the portion of a segment that has been allocated to a log write.
 * The constructor leaves the fields uninitialized for population by CommitlogManager, so that it can be
 * stack-allocated by escape analysis in CommitLog.add.
 */
class Allocation
{

    private final CommitLogSegment segment;
    private final OpOrder.Group appendOp;
    private final ReplayPosition position;
    private final ByteBuffer buffer;

    Allocation(CommitLogSegment segment, OpOrder.Group appendOp, ReplayPosition position, ByteBuffer buffer)
    {
        this.segment = segment;
        this.appendOp = appendOp;
        this.position = position;
        this.buffer = buffer;
    }

    CommitLogSegment getSegment()
    {
        return segment;
    }

    ByteBuffer getBuffer()
    {
        return buffer;
    }

    // markWritten() MUST be called once we are done with the segment or the CL will never flush
    // but must not be called more than once
    void markWritten()
    {
        appendOp.close();
    }

    void awaitDiskSync()
    {
        while (segment.lastSyncedOffset < position.position)
        {
            WaitQueue.Signal signal = segment.syncComplete.register(CommitLog.instance.metrics.waitingOnCommit.time());
            if (segment.lastSyncedOffset < position.position)
                signal.awaitUninterruptibly();
            else
                signal.cancel();
        }
    }

    public ReplayPosition getReplayPosition()
    {
        return position;
    }

}