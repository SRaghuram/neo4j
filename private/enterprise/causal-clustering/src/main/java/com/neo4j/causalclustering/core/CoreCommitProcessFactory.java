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

import org.neo4j.configuration.helpers.ReadOnlyDatabaseChecker;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.InternalTransactionCommitProcess;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.storageengine.api.StorageEngine;

public class CoreCommitProcessFactory implements CommitProcessFactory
{
    private final Replicator replicator;
    private final CoreStateMachines coreStateMachines;
    private final ClusterLeaseCoordinator leaseCoordinator;
    private final LogEntryWriterFactory logEntryWriterFactory;

    CoreCommitProcessFactory( Replicator replicator, CoreStateMachines coreStateMachines,
                              ClusterLeaseCoordinator leaseCoordinator, LogEntryWriterFactory logEntryWriterFactory )
    {
        this.replicator = replicator;
        this.coreStateMachines = coreStateMachines;
        this.leaseCoordinator = leaseCoordinator;
        this.logEntryWriterFactory = logEntryWriterFactory;
    }

    @Override
    public TransactionCommitProcess create( TransactionAppender appender, StorageEngine storageEngine,
            NamedDatabaseId databaseId, ReadOnlyDatabaseChecker readOnlyDatabaseChecker )
    {
        initializeCommitProcessForStateMachines( appender, storageEngine );
        return new ReplicatedTransactionCommitProcess( replicator, databaseId, leaseCoordinator, logEntryWriterFactory, readOnlyDatabaseChecker );
    }

    /**
     * Instantiates a simple Kernel commit process and injects it into {@link #coreStateMachines}.
     * This is required because real transaction commit in CC happens only through state machines.
     */
    private void initializeCommitProcessForStateMachines( TransactionAppender appender, StorageEngine storageEngine )
    {
        var commitProcess = new InternalTransactionCommitProcess( appender, storageEngine );
        coreStateMachines.installCommitProcess( commitProcess );
    }
}
