/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.state.CoreStateService;
import com.neo4j.causalclustering.core.state.PerDatabaseCoreStateComponents;

import java.util.function.Function;

import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseContext;
import org.neo4j.graphdb.factory.module.id.DatabaseIdContext;
import org.neo4j.io.fs.watcher.DatabaseLayoutWatcher;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.factory.StatementLocksFactorySelector;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.logging.internal.LogService;
import org.neo4j.time.SystemNanoClock;

public class CoreDatabaseContext implements EditionDatabaseContext
{
    private final CoreEditionModule editionModule;
    private final PerDatabaseCoreStateComponents databaseState;
    private final StatementLocksFactory statementLocksFactory;
    private final DatabaseTransactionStats transactionMonitor;
    private final String databaseName;

    public CoreDatabaseContext( GlobalModule globalModule, CoreEditionModule editionModule, String databaseName )
    {
        this.databaseName =  databaseName;
        this.editionModule = editionModule;
        CoreStateService coreStateService = editionModule.coreStateComponents();
        databaseState = coreStateService.getDatabaseState( databaseName )
                .orElseThrow( () -> new IllegalStateException( String.format( "There is no state found for the database %s", databaseName ) ) );
        statementLocksFactory = new StatementLocksFactorySelector( databaseState.lockManager(), globalModule.getGlobalConfig(),
                globalModule.getLogService() ).select();
        transactionMonitor = editionModule.createTransactionMonitor();
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
    public SchemaWriteGuard getSchemaWriteGuard()
    {
        return editionModule.getSchemaWriteGuard();
    }

    @Override
    public long getTransactionStartTimeout()
    {
        return editionModule.getTransactionStartTimeout();
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
    public DatabaseAvailabilityGuard createDatabaseAvailabilityGuard( SystemNanoClock clock, LogService logService, Config config )
    {
        //TODO: In general, not happy that responsibility for creation of this lies in the creation context.
        // I would prefer the following: responsibility for the creation of per db availability guards is passed to the DatabaseService#register methods
        // The databases service would then become a field/getter on the edition module (much like CoreStateService) and we would just get() it in
        // the constructor here. Could actually then further cleanup by making LocalDatabase objects carry their own PerDatabaseCoreStateComponents.
        return editionModule.createDatabaseAvailabilityGuard( databaseName, clock, logService, config );
    }
}
