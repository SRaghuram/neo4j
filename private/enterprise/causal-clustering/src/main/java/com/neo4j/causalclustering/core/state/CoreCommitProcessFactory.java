/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.replication.Replicator;
import com.neo4j.causalclustering.core.state.machines.CoreStateMachines;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionCommitProcess;
import com.neo4j.causalclustering.error_handling.Panicker;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.storageengine.api.StorageEngine;

class CoreCommitProcessFactory implements CommitProcessFactory
{
    private final DatabaseId databaseId;
    private final Replicator replicator;
    private final CoreStateMachines coreStateMachines;
    private final Panicker panicker;

    CoreCommitProcessFactory( DatabaseId databaseId, Replicator replicator, CoreStateMachines coreStateMachines, Panicker panicker )
    {
        this.databaseId = databaseId;
        this.replicator = replicator;
        this.coreStateMachines = coreStateMachines;
        this.panicker = panicker;
    }

    @Override
    public TransactionCommitProcess create( TransactionAppender appender, StorageEngine storageEngine, Config config )
    {
        initializeCommitProcessForStateMachines( appender, storageEngine );
        return new ReplicatedTransactionCommitProcess( replicator, databaseId, panicker );
    }

    /**
     * Instantiates a simple Kernel commit process and injects it into {@link #coreStateMachines}.
     * This is required because real transaction commit in CC happens only through state machines.
     */
    private void initializeCommitProcessForStateMachines( TransactionAppender appender, StorageEngine storageEngine )
    {
        var commitProcess = new TransactionRepresentationCommitProcess( appender, storageEngine );
        coreStateMachines.installCommitProcess( commitProcess );
    }
}
