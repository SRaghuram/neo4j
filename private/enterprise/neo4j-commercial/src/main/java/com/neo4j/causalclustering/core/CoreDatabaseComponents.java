/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.state.CoreStateService;
import com.neo4j.causalclustering.core.state.DatabaseCoreStateComponents;

import java.util.function.Function;

import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseComponents;
import org.neo4j.graphdb.factory.module.id.DatabaseIdContext;
import org.neo4j.io.fs.watcher.DatabaseLayoutWatcher;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.factory.StatementLocksFactorySelector;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.token.TokenHolders;

public class CoreDatabaseComponents implements EditionDatabaseComponents
{
    private final CoreEditionModule editionModule;
    private final DatabaseCoreStateComponents databaseState;
    private final StatementLocksFactory statementLocksFactory;
    private final DatabaseTransactionStats transactionMonitor;
    private final long startTimeoutMillis;

    CoreDatabaseComponents( GlobalModule globalModule, CoreEditionModule editionModule, DatabaseId databaseId )
    {
        this.editionModule = editionModule;
        CoreStateService coreStateService = editionModule.coreStateService();
        databaseState = coreStateService.getDatabaseState( databaseId )
                .orElseThrow( () -> new IllegalStateException( String.format( "There is no state found for the database %s", databaseId.name() ) ) );
        statementLocksFactory = new StatementLocksFactorySelector( databaseState.lockManager(), globalModule.getGlobalConfig(),
                globalModule.getLogService() ).select();
        transactionMonitor = editionModule.createTransactionMonitor();
        startTimeoutMillis = editionModule.getTransactionStartTimeout();
    }

    @Override
    public DatabaseIdContext getIdContext()
    {
        return databaseState.idContext();
    }

    @Override
    public TokenHolders getTokenHolders()
    {
        return databaseState.tokenHolders();
    }

    @Override
    public Function<DatabaseLayout,DatabaseLayoutWatcher> getWatcherServiceFactory()
    {
        return editionModule.getWatcherServiceFactory();
    }

    @Override
    public AccessCapability getAccessCapability()
    {
        return editionModule.getAccessCapability();
    }

    @Override
    public IOLimiter getIoLimiter()
    {
        return editionModule.getIoLimiter();
    }

    @Override
    public ConstraintSemantics getConstraintSemantics()
    {
        return editionModule.getConstraintSemantics();
    }

    @Override
    public CommitProcessFactory getCommitProcessFactory()
    {
        return databaseState.commitProcessFactory();
    }

    @Override
    public TransactionHeaderInformationFactory getHeaderInformationFactory()
    {
        return editionModule.getHeaderInformationFactory();
    }

    @Override
    public Locks getLocks()
    {
        return databaseState.lockManager();
    }

    @Override
    public StatementLocksFactory getStatementLocksFactory()
    {
        return statementLocksFactory;
    }

    @Override
    public DatabaseTransactionStats getTransactionMonitor()
    {
        return transactionMonitor;
    }

    @Override
    public long getStartTimeoutMillis()
    {
        return startTimeoutMillis;
    }
}
