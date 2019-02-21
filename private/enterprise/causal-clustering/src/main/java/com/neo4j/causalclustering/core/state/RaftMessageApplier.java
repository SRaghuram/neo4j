/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.outcome.ConsensusOutcome;
import com.neo4j.causalclustering.core.state.snapshot.CoreDownloaderService;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;

import java.util.Optional;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobHandle;

public class RaftMessageApplier implements LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>>
{
    private final Log log;
    private final RaftMachine raftMachine;
    private final CoreDownloaderService downloadService;
    private final CommandApplicationProcess applicationProcess;
    private CatchupAddressProvider.LeaderOrUpstreamStrategyBasedAddressProvider catchupAddressProvider;
    private final Panicker panicker;
    private boolean stopped;

    public RaftMessageApplier( LogProvider logProvider, RaftMachine raftMachine, CoreDownloaderService downloadService,
            CommandApplicationProcess applicationProcess, CatchupAddressProvider.LeaderOrUpstreamStrategyBasedAddressProvider catchupAddressProvider,
            Panicker panicker )
    {
        this.log = logProvider.getLog( getClass() );
        this.raftMachine = raftMachine;
        this.downloadService = downloadService;
        this.applicationProcess = applicationProcess;
        this.catchupAddressProvider = catchupAddressProvider;
        this.panicker = panicker;
    }

    @Override
    public synchronized void handle( RaftMessages.ReceivedInstantClusterIdAwareMessage<?> wrappedMessage )
    {
        // TODO: At tme moment download jobs are issued against both databases every time they are issued at all. This is probably fine for 3.5, but should be
        // fixed for 4.0
        if ( stopped )
        {
            return;
        }
        try
        {
            ConsensusOutcome outcome = raftMachine.handle( wrappedMessage.message() );
            if ( outcome.needsFreshSnapshot() )
            {
                Optional<JobHandle> downloadJob = downloadService.scheduleDownload( catchupAddressProvider );
                if ( downloadJob.isPresent() )
                {
                    downloadJob.get().waitTermination();
                }
            }
            else
            {
                notifyCommitted( outcome.getCommitIndex() );
            }
        }
        catch ( Throwable e )
        {
            log.error( "Error handling message", e );
            panicker.panic( e );
            stop();
        }
    }

    @Override
    public synchronized void start( ClusterId clusterId )
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
