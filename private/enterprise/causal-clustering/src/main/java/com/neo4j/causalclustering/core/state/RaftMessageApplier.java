/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.outcome.ConsensusOutcome;
import com.neo4j.causalclustering.core.consensus.outcome.SnapshotRequirement;
import com.neo4j.causalclustering.core.state.snapshot.CoreDownloaderService;
import com.neo4j.causalclustering.error_handling.DatabasePanicReason;
import com.neo4j.causalclustering.error_handling.DatabasePanicker;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobHandle;

import static java.lang.String.format;

public class RaftMessageApplier implements LifecycleMessageHandler<RaftMessages.InboundRaftMessageContainer<?>>
{
    private final Log log;
    private final RaftMachine raftMachine;
    private final CoreDownloaderService downloadService;
    private final CommandApplicationProcess applicationProcess;
    private final CatchupAddressProvider.LeaderOrUpstreamStrategyBasedAddressProvider catchupAddressProvider;
    private final DatabasePanicker panicker;
    private boolean stopped;

    public RaftMessageApplier( LogProvider logProvider, RaftMachine raftMachine, CoreDownloaderService downloadService,
                               CommandApplicationProcess applicationProcess,
                               CatchupAddressProvider.LeaderOrUpstreamStrategyBasedAddressProvider catchupAddressProvider,
                               DatabasePanicker panicker )
    {
        this.log = logProvider.getLog( getClass() );
        this.raftMachine = raftMachine;
        this.downloadService = downloadService;
        this.applicationProcess = applicationProcess;
        this.catchupAddressProvider = catchupAddressProvider;
        this.panicker = panicker;
    }

    @Override
    public synchronized void handle( RaftMessages.InboundRaftMessageContainer<?> wrappedMessage )
    {
        if ( stopped )
        {
            return;
        }

        try
        {
            ConsensusOutcome outcome = raftMachine.handle( wrappedMessage.message() );
            if ( outcome.snapshotRequirement().isPresent() )
            {
                SnapshotRequirement snapshotRequirement = outcome.snapshotRequirement().get();
                log.info( format( "Scheduling download because of %s", snapshotRequirement ) );
                scheduleAndAwaitDownload();
            }
            else
            {
                notifyCommitted( outcome.getCommitIndex() );
            }
        }
        catch ( Throwable e )
        {
            log.error( "Error handling message", e );
            panicker.panic( DatabasePanicReason.RaftMessageApplierFailed, e );
            stopped = true;
        }
    }

    private void scheduleAndAwaitDownload() throws InterruptedException, ExecutionException
    {
        Optional<JobHandle<?>> downloadJob = downloadService.scheduleDownload( catchupAddressProvider );
        if ( downloadJob.isPresent() )
        {
            downloadJob.get().waitTermination();
        }
    }

    @Override
    public synchronized void start( RaftGroupId raftGroupId )
    {
        stopped = false;
    }

    @Override
    public synchronized void stop()
    {
        stopped = true;
    }

    private void notifyCommitted( long commitIndex )
    {
        applicationProcess.notifyCommitted( commitIndex );
    }
}
