/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.SessionTracker;
import com.neo4j.causalclustering.core.CoreState;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import com.neo4j.causalclustering.core.replication.DistributedOperation;
import com.neo4j.causalclustering.core.replication.ProgressTrackerImpl;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.core.replication.session.GlobalSession;
import com.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import com.neo4j.causalclustering.core.replication.session.LocalOperationId;
import com.neo4j.causalclustering.core.state.machines.tx.CoreReplicatedContent;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import com.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import com.neo4j.causalclustering.error_handling.DatabasePanicker;
import com.neo4j.causalclustering.identity.IdFactory;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.UUID;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class RaftLogPrunerTest
{
    private final InMemoryRaftLog raftLog = new InMemoryRaftLog();
    private final SessionTracker sessionTracker = new SessionTracker( new InMemoryStateStorage<>( new GlobalSessionTrackerState() ) );

    private final GlobalSession globalSession = new GlobalSession( UUID.randomUUID(), IdFactory.randomRaftMemberId() );
    private final int flushEvery = 10;
    private final int batchSize = 16;

    private final DatabaseId databaseId = new TestDatabaseIdRepository().defaultDatabase().databaseId();
    private final InFlightCache inFlightCache = new ConsecutiveInFlightCache();
    private final Monitors monitors = new Monitors();
    private final CoreState coreState = mock( CoreState.class );
    private final JobScheduler jobScheduler = new ThreadPoolJobScheduler();
    private final DatabasePanicker panickier = Mockito.mock( DatabasePanicker.class );
    private final CommandApplicationProcess applicationProcess =
            new CommandApplicationProcess( raftLog, batchSize, flushEvery, NullLogProvider.getInstance(), new ProgressTrackerImpl( globalSession ),
                                           sessionTracker, coreState, inFlightCache, monitors, panickier, jobScheduler );

    private final ReplicatedTransaction nullTx = ReplicatedTransaction.from( new byte[0], databaseId );
    private int sequenceNumber;

    @Test
    void shouldPruneAllApplied() throws Exception
    {
        final var raftMachine = mock( RaftMachine.class );
        RaftLogPruner logPruner = new RaftLogPruner( raftMachine, applicationProcess );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );
        raftLog.append( new RaftLogEntry( 0, operation( nullTx ) ) );

        final var expected = raftLog.appendIndex();
        assertThat( (long) flushEvery ).isGreaterThan( expected );

        applicationProcess.notifyCommitted( expected );
        applicationProcess.start();

        //when
        logPruner.prune();

        //then
        verify( raftMachine ).handle( new RaftMessages.PruneRequest( expected ) );
    }

    private ReplicatedContent operation( CoreReplicatedContent tx )
    {
        return new DistributedOperation( tx, globalSession, new LocalOperationId( 0, sequenceNumber++ ) );
    }
}
