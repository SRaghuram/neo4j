/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.replication.Replicator;
import com.neo4j.causalclustering.core.state.machines.CoreStateMachines;
import com.neo4j.causalclustering.core.state.machines.lease.ClusterLeaseCoordinator;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionCommitProcess;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.ReadOnlyDatabaseChecker;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.api.InternalTransactionCommitProcess;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.storageengine.api.StorageEngine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class CoreCommitProcessFactoryTest
{
    private final NamedDatabaseId namedDatabaseId = TestDatabaseIdRepository.randomNamedDatabaseId();
    private final Replicator replicator = mock( Replicator.class );
    private final CoreStateMachines coreStateMachines = mock( CoreStateMachines.class );
    private final ClusterLeaseCoordinator leaseCoordinator = mock( ClusterLeaseCoordinator.class );

    private final TransactionAppender appender = mock( TransactionAppender.class );
    private final StorageEngine storageEngine = mock( StorageEngine.class );
    private final Config config = Config.defaults();
    private final ReadOnlyDatabaseChecker readOnlyDatabaseChecker = new ReadOnlyDatabaseChecker.Default( config );

    private final CoreCommitProcessFactory commitProcessFactory = new CoreCommitProcessFactory( replicator, coreStateMachines,
                                                                                                leaseCoordinator, LogEntryWriterFactory.LATEST );

    @Test
    void shouldCreateReplicatedCommitProcess()
    {
        var commitProcess = commitProcessFactory.create( appender, storageEngine, namedDatabaseId, readOnlyDatabaseChecker );
        assertThat( commitProcess, Matchers.instanceOf( ReplicatedTransactionCommitProcess.class ) );
    }

    @Test
    void shouldInstallCommitProcess()
    {
        commitProcessFactory.create( appender, storageEngine, namedDatabaseId, readOnlyDatabaseChecker );
        verify( coreStateMachines ).installCommitProcess( any( InternalTransactionCommitProcess.class ) );
    }
}
