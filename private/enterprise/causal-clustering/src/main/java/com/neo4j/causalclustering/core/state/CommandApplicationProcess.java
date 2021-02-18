/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.SessionTracker;
import com.neo4j.causalclustering.core.CoreState;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import com.neo4j.causalclustering.core.consensus.log.monitoring.RaftLogAppliedIndexMonitor;
import com.neo4j.causalclustering.core.consensus.log.monitoring.RaftLogCommitIndexMonitor;
import com.neo4j.causalclustering.core.replication.DistributedOperation;
import com.neo4j.causalclustering.core.replication.ProgressTracker;
import com.neo4j.causalclustering.core.state.machines.tx.CoreReplicatedContent;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.error_handling.DatabasePanicEvent;
import com.neo4j.causalclustering.error_handling.DatabasePanicEventHandler;
import com.neo4j.causalclustering.error_handling.DatabasePanicReason;
import com.neo4j.causalclustering.error_handling.DatabasePanicker;
import com.neo4j.causalclustering.helper.StatUtil;
import com.neo4j.causalclustering.helper.scheduling.LimitingScheduler;
import com.neo4j.causalclustering.helper.scheduling.SingleElementJobsQueue;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.util.locks.CriticalSection;

import static java.lang.Math.max;
import static java.lang.String.format;

public class CommandApplicationProcess implements DatabasePanicEventHandler
{
    private static final long NOTHING = -1;
    private final RaftLog raftLog;
    private final int flushEvery;
    private final ProgressTracker progressTracker;
    private final SessionTracker sessionTracker;
    private final CoreState coreState;
    private final InFlightCache inFlightCache;
    private final Log log;
    private final RaftLogCommitIndexMonitor commitIndexMonitor;
    private final RaftLogAppliedIndexMonitor appliedIndexMonitor;
    private final CommandBatcher batcher;
    private final DatabasePanicker panicker;
    private final Supplier<LimitingScheduler> schedulerSupplier;
    private LimitingScheduler scheduler;
    private final StatUtil.StatContext batchStat;

    private final AtomicLong lastFlushed = new AtomicLong( NOTHING );
    private int pauseCount = 1; // we are created in the paused state
    private final ApplierState applierState = new ApplierState();
    private volatile boolean hasPanicked;

    private final CriticalSection control = new CriticalSection();
    private final CriticalSection data = new CriticalSection();

    public CommandApplicationProcess( RaftLog raftLog, int maxBatchSize, int flushEvery, LogProvider logProvider, ProgressTracker progressTracker,
                                      SessionTracker sessionTracker, CoreState coreState, InFlightCache inFlightCache, Monitors monitors,
                                      DatabasePanicker panicker, JobScheduler scheduler )
    {
        this.raftLog = raftLog;
        this.flushEvery = flushEvery;
        this.progressTracker = progressTracker;
        this.sessionTracker = sessionTracker;
        this.log = logProvider.getLog( getClass() );
        this.coreState = coreState;
        this.inFlightCache = inFlightCache;
        this.commitIndexMonitor = monitors.newMonitor( RaftLogCommitIndexMonitor.class, getClass().getName() );
        this.appliedIndexMonitor = monitors.newMonitor( RaftLogAppliedIndexMonitor.class, getClass().getName() );
        this.batcher = new CommandBatcher( maxBatchSize, this::applyBatch );
        this.panicker = panicker;
        this.schedulerSupplier = () -> new LimitingScheduler( scheduler, Group.CORE_STATE_APPLIER, log, new SingleElementJobsQueue<>() );
        this.batchStat = StatUtil.create( "BatchSize", log, 4096, true );
    }

    void notifyCommitted( long commitIndex )
    {
        applierState.notifyCommitted( commitIndex );
        if ( applierState.toApply() != NOTHING )
        {
            scheduleJob();
        }
    }

    private void scheduleJob()
    {
        control.lock( () ->
                      {
                          if ( pauseCount == 0 && !hasPanicked )
                          {
                              scheduler.offerJob( this::applyJob );
                          }
                      } );
    }

    @Override
    public void onPanic( DatabasePanicEvent panic )
    {
        hasPanicked = true;
    }

    private class ApplierState
    {
        // core applier state, synchronized by ApplierState monitor
        private long lastSeenCommitIndex = NOTHING;

        // owned by applier
        private volatile long lastApplied = NOTHING;

        private synchronized long getLastSeenCommitIndex()
        {
            return lastSeenCommitIndex;
        }

        synchronized long toApply()
        {
            return lastApplied >= lastSeenCommitIndex ? NOTHING : lastSeenCommitIndex;
        }

        synchronized void notifyCommitted( long commitIndex )
        {
            if ( lastSeenCommitIndex < commitIndex )
            {
                lastSeenCommitIndex = commitIndex;
                commitIndexMonitor.commitIndex( commitIndex );
            }
        }

        void setLastApplied( long lastApplied )
        {
            this.lastApplied = lastApplied;
            appliedIndexMonitor.appliedIndex( lastApplied );
        }
    }

    private void applyJob()
    {
        try
        {
            applyUpTo( applierState.toApply() );
        }
        catch ( Throwable e )
        {
            panicker.panic( DatabasePanicReason.COMMAND_APPLICATION_FAILED, e );
            log.error( "Failed to apply", e );
        }
    }

    private void applyUpTo( long applyUpToIndex ) throws Exception
    {
        if ( applyUpToIndex == NOTHING )
        {
            // nothing to apply
            return;
        }
        try ( InFlightLogEntryReader logEntrySupplier = new InFlightLogEntryReader( raftLog, inFlightCache, true ) )
        {
            for ( long logIndex = applierState.lastApplied + 1; !hasPanicked && logIndex <= applyUpToIndex; logIndex++ )
            {
                RaftLogEntry entry = logEntrySupplier.get( logIndex );
                if ( entry == null )
                {
                    throw new IllegalStateException( format( "Committed log entry at index %d must exist.", logIndex ) );
                }

                if ( entry.content() instanceof DistributedOperation )
                {
                    DistributedOperation distributedOperation = (DistributedOperation) entry.content();
                    progressTracker.trackReplication( distributedOperation );
                    batcher.add( logIndex, distributedOperation );
                }
                else
                {
                    batcher.flush();
                    // since this last entry didn't get in the batcher we need to update the lastApplied:
                    applierState.setLastApplied( logIndex );
                }
            }
            batcher.flush();
        }
    }

    long lastApplied()
    {
        return applierState.lastApplied;
    }

    /**
     * The applier must be paused when installing a snapshot.
     *
     * @param coreSnapshot The snapshot to install.
     */
    void installSnapshot( CoreSnapshot coreSnapshot )
    {
        control.lock( () ->
                      {
                          if ( pauseCount <= 0 )
                          {
                              throw new IllegalStateException( "Must be paused when installing a snapshot" );
                          }
                          lastFlushed.set( coreSnapshot.prevIndex() );
                          applierState.setLastApplied( coreSnapshot.prevIndex() );
                      } );
    }

    private void applyBatch( long lastIndex, List<DistributedOperation> batch ) throws Exception
    {
        if ( batch.size() == 0 )
        {
            return;
        }

        batchStat.collect( batch.size() );

        data.lock( () ->
                   {
                       long startIndex = lastIndex - batch.size() + 1;
                       long lastHandledIndex = handleOperations( startIndex, batch );
                       assert lastHandledIndex == lastIndex;
                       applierState.setLastApplied( lastIndex );

                       maybeFlushToDisk();
                   } );
    }

    private long handleOperations( long commandIndex, List<DistributedOperation> operations )
    {
        try ( CommandDispatcher dispatcher = coreState.commandDispatcher() )
        {
            for ( DistributedOperation operation : operations )
            {
                if ( !sessionTracker.validateOperation( operation.globalSession(), operation.operationId() ) )
                {
                    if ( log.isDebugEnabled() )
                    {
                        log.debug( "Skipped an invalid distributed operation: " + operation + ". Session tracker state: " + sessionTracker.snapshot() );
                    }
                    commandIndex++;
                    continue;
                }

                CoreReplicatedContent command = (CoreReplicatedContent) operation.content();
                command.dispatch( dispatcher, commandIndex,
                                  result -> progressTracker.trackResult( operation, result ) );

                sessionTracker.update( operation.globalSession(), operation.operationId(), commandIndex );
                commandIndex++;
            }
        }
        return commandIndex - 1;
    }

    private void maybeFlushToDisk() throws IOException
    {
        if ( (applierState.lastApplied - lastFlushed.get()) > flushEvery )
        {
            doFlush();
        }
    }

    private long doFlush() throws IOException
    {
        return data.lock( () ->
                          {
                              long flushIndex = applierState.lastApplied;
                              if ( flushIndex > NOTHING )
                              {
                                  coreState.flush( flushIndex );
                                  lastFlushed.set( flushIndex );
                              }
                              return flushIndex;
                          } );
    }

    public long flush() throws IOException
    {
        return control.lock( () ->
                             {
                                 if ( pauseCount > 0 )
                                 {
                                     return NOTHING;
                                 }
                                 return doFlush();
                             } );
    }

    public void start() throws Exception
    {
        control.lock( () ->
                      {
                          lastFlushed.compareAndSet( NOTHING, coreState.getLastFlushed() );
                          applierState.setLastApplied( lastFlushed.get() );

                          log.info( format( "Restoring last applied index to %d", lastFlushed.get() ) );
                          sessionTracker.start();

                          /* Considering the order in which state is flushed, the state machines will
                           * always be furthest ahead and indicate the furthest possible state to
                           * which we must replay to reach a consistent state. */
                          long lastPossiblyApplying = max( coreState.getLastAppliedIndex(), applierState.getLastSeenCommitIndex() );

                          if ( lastPossiblyApplying > applierState.lastApplied )
                          {
                              log.info( "Applying up to: " + lastPossiblyApplying );
                              applyUpTo( lastPossiblyApplying );
                          }

                          resumeApplier( "startup" );
                      } );
    }

    public void stop() throws IOException
    {
        control.lock( () ->
                      {
                          pauseApplier( "shutdown" );
                          doFlush();
                      } );
    }

    public void pauseApplier( String reason )
    {
        control.lock( () ->
                      {
                          if ( pauseCount < 0 )
                          {
                              throw new IllegalStateException( "Unmatched pause/resume" );
                          }

                          pauseCount++;
                          log.info( format( "Pausing due to %s (count = %d)", reason, pauseCount ) );

                          if ( scheduler != null )
                          {
                              scheduler.stopAndFlush();
                              scheduler = null;
                          }
                      } );
    }

    public void resumeApplier( String reason )
    {
        control.lock( () ->
                      {
                          if ( pauseCount <= 0 )
                          {
                              throw new IllegalStateException( "Unmatched pause/resume" );
                          }

                          pauseCount--;
                          log.info( format( "Resuming after %s (count = %d)", reason, pauseCount ) );

                          if ( pauseCount == 0 )
                          {
                              assert scheduler == null;
                              scheduler = schedulerSupplier.get();
                              scheduleJob();
                          }
                      } );
    }
}
