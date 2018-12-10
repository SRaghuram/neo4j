/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.causalclustering.common.AbstractLocalDatabase;
import com.neo4j.causalclustering.core.state.PerDatabaseCoreStateComponents;

import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StorageEngine;

public final class CoreLocalDatabase extends AbstractLocalDatabase
{
    private TransactionRepresentationCommitProcess commitProcess;
    private PerDatabaseCoreStateComponents databaseState;

    public CoreLocalDatabase( String databaseName, Supplier<DatabaseManager> databaseManagerSupplier, DatabaseLayout databaseLayout, LogFiles txLogs,
            StoreFiles storeFiles, LogProvider logProvider, BooleanSupplier isAvailable )
    {
        super( databaseName, databaseManagerSupplier, databaseLayout, txLogs, storeFiles, logProvider, isAvailable );
    }

    public void setCommitProcessDependencies( TransactionAppender appender, StorageEngine storageEngine )
    {
        this.commitProcess = new TransactionRepresentationCommitProcess( appender, storageEngine );
    }

    public TransactionRepresentationCommitProcess commitProcess()
    {
        return commitProcess;
    }

    public void setCoreStateComponents( PerDatabaseCoreStateComponents databaseState )
    {
        this.databaseState = databaseState;
    }

    @Override
    public void start0()
    {
        if ( commitProcess == null || databaseState == null )
        {
            throw new IllegalStateException( "Cannot start a database without first providing a commit process and core state components" );
        }
        databaseState.stateMachines().installCommitProcess( commitProcess );
    }
}
