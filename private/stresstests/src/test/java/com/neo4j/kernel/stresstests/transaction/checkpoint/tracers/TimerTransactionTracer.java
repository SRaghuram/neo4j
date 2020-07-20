/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.stresstests.transaction.checkpoint.tracers;

import org.HdrHistogram.Histogram;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogFileCreateEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceWaitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogRotateEvent;
import org.neo4j.kernel.impl.transaction.tracing.SerializeTransactionEvent;
import org.neo4j.kernel.impl.transaction.tracing.StoreApplyEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;

public class TimerTransactionTracer implements DatabaseTracer
{
    private static volatile long logForceBegin;
    private static volatile long logCheckPointBegin;
    private static volatile long logRotateBegin;
    private static final Histogram logForceTimes = new Histogram( 1000, TimeUnit.MINUTES.toNanos( 45 ), 0 );
    private static final Histogram logRotateTimes = new Histogram( 1000, TimeUnit.MINUTES.toNanos( 45 ), 0 );
    private static final Histogram logCheckPointTimes = new Histogram( 1000, TimeUnit.MINUTES.toNanos( 45 ), 0 );

    public static void printStats( PrintStream out )
    {
        printStat( out, "Log force millisecond percentiles:", logForceTimes );
        printStat( out, "Log rotate millisecond percentiles:", logRotateTimes );
        printStat( out, "Log check point millisecond percentiles:", logCheckPointTimes );
    }

    private static void printStat( PrintStream out, String message, Histogram histogram )
    {
        out.println( message );
        histogram.outputPercentileDistribution( out, 1000000.0 );
        out.println();
    }

    private static final LogForceEvent LOG_FORCE_EVENT = new LogForceEvent()
    {
        @Override
        public void close()
        {
            long elapsedNanos = System.nanoTime() - logForceBegin;
            logForceTimes.recordValue( elapsedNanos );
        }
    };

    private static final LogCheckPointEvent LOG_CHECK_POINT_EVENT = new LogCheckPointEvent()
    {
        @Override
        public LogRotateEvent beginLogRotate()
        {
            return LogRotateEvent.NULL;
        }

        @Override
        public LogForceWaitEvent beginLogForceWait()
        {
            return LogForceWaitEvent.NULL;
        }

        @Override
        public LogForceEvent beginLogForce()
        {
            logForceBegin = System.nanoTime();
            return LOG_FORCE_EVENT;
        }

        @Override
        public void checkpointCompleted( long checkpointMillis )
        {
        }

        @Override
        public void close()
        {
            long elapsedNanos = System.nanoTime() - logCheckPointBegin;
            logCheckPointTimes.recordValue( elapsedNanos );
        }

        @Override
        public void appendToLogFile( LogPosition positionBeforeCheckpoint, LogPosition positionAfterCheckpoint )
        {

        }
    };

    private static final LogRotateEvent LOG_ROTATE_EVENT = new LogRotateEvent()
    {
        @Override
        public void rotationCompleted( long rotationMillis )
        {
            long elapsedNanos = System.nanoTime() - logRotateBegin;
            logRotateTimes.recordValue( elapsedNanos );
        }

        @Override
        public void close()
        {
        }
    };

    private static final LogAppendEvent LOG_APPEND_EVENT = new LogAppendEvent()
    {
        @Override
        public void appendToLogFile( LogPosition logPositionBeforeAppend, LogPosition logPositionAfterAppend )
        {

        }

        @Override
        public void close()
        {
        }

        @Override
        public void setLogRotated( boolean b )
        {
        }

        @Override
        public LogRotateEvent beginLogRotate()
        {
            logRotateBegin = System.nanoTime();
            return LOG_ROTATE_EVENT;
        }

        @Override
        public SerializeTransactionEvent beginSerializeTransaction()
        {
            return SerializeTransactionEvent.NULL;
        }

        @Override
        public LogForceWaitEvent beginLogForceWait()
        {
            return LogForceWaitEvent.NULL;
        }

        @Override
        public LogForceEvent beginLogForce()
        {
            logForceBegin = System.nanoTime();
            return LOG_FORCE_EVENT;
        }
    };

    private static final CommitEvent COMMIT_EVENT = new CommitEvent()
    {
        @Override
        public void close()
        {
        }

        @Override
        public LogAppendEvent beginLogAppend()
        {
            return LOG_APPEND_EVENT;
        }

        @Override
        public StoreApplyEvent beginStoreApply()
        {
            return StoreApplyEvent.NULL;
        }
    };

    private static final TransactionEvent TRANSACTION_EVENT = new TransactionEvent()
    {
        @Override
        public void setSuccess( boolean b )
        {
        }

        @Override
        public void setFailure( boolean b )
        {
        }

        @Override
        public CommitEvent beginCommitEvent()
        {
            return COMMIT_EVENT;
        }

        @Override
        public void close()
        {
        }

        @Override
        public void setTransactionWriteState( String transactionWriteState )
        {
        }

        @Override
        public void setReadOnly( boolean b )
        {
        }
    };

    @Override
    public TransactionEvent beginTransaction( PageCursorTracer cursorTracer )
    {
        return TRANSACTION_EVENT;
    }

    @Override
    public LogCheckPointEvent beginCheckPoint()
    {
        logCheckPointBegin = System.nanoTime();
        return LOG_CHECK_POINT_EVENT;
    }

    @Override
    public long appendedBytes()
    {
        return 0;
    }

    @Override
    public long numberOfLogRotations()
    {
        return 0;
    }

    @Override
    public long logRotationAccumulatedTotalTimeMillis()
    {
        return 0;
    }

    @Override
    public long lastLogRotationTimeMillis()
    {
        return 0;
    }

    @Override
    public LogFileCreateEvent createLogFile()
    {
        return LogFileCreateEvent.NULL;
    }

    @Override
    public long numberOfCheckPoints()
    {
        return 0;
    }

    @Override
    public long checkPointAccumulatedTotalTimeMillis()
    {
        return 0;
    }

    @Override
    public long lastCheckpointTimeMillis()
    {
        return 0;
    }
}
