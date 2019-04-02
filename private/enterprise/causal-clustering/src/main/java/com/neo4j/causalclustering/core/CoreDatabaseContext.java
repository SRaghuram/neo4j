/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.catchup.CatchupComponentsFactory;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.causalclustering.common.AbstractClusteredDatabaseContext;
import com.neo4j.causalclustering.core.state.CoreStateService;
import com.neo4j.causalclustering.core.state.DatabaseCoreStateComponents;

import java.util.function.BooleanSupplier;

import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.LogProvider;

public final class CoreDatabaseContext extends AbstractClusteredDatabaseContext
{
    private final CoreStateService coreStateService;
    private TransactionRepresentationCommitProcess commitProcess;
    private DatabaseCoreStateComponents databaseState;

    public CoreDatabaseContext( Database database, GraphDatabaseFacade facade, LogFiles txLogs, StoreFiles storeFiles, LogProvider logProvider,
            BooleanSupplier isAvailable, CoreStateService coreStateService, CatchupComponentsFactory catchupComponentsFactory )
    {
        super( database, facade, txLogs, storeFiles, logProvider, isAvailable, catchupComponentsFactory );
        this.coreStateService = coreStateService;
        this.databaseState = coreStateService.getDatabaseState( databaseId() ).orElseThrow( IllegalStateException::new );
    }

    void setCommitProcess( TransactionRepresentationCommitProcess commitProcess )
    {
        this.commitProcess = commitProcess;
    }

    @Override
    public void start0()
    {
        if ( commitProcess == null )
        {
            throw new IllegalStateException( "Cannot start a database without first providing a commit process" );
        }
        databaseState.stateMachines().installCommitProcess( commitProcess );
    }

    @Override
    public void stop0()
    {
        coreStateService.remove( databaseId() );
    }
}
