/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseComponents;
import org.neo4j.graphdb.factory.module.id.DatabaseIdContext;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder;
import org.neo4j.io.fs.watcher.DatabaseLayoutWatcher;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseNameLogContext;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.ReadOnlyTransactionCommitProcess;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.factory.AccessCapabilityFactory;
import org.neo4j.kernel.impl.factory.ReadOnly;
import org.neo4j.kernel.impl.factory.StatementLocksFactorySelector;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.logging.internal.DatabaseLogService;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

public class ReadReplicaDatabaseComponents implements EditionDatabaseComponents
{
    private final Locks locksManager;
    private final StatementLocksFactory statementLocksFactory;
    private final DatabaseIdContext idContext;
    private final TokenHolders tokenHolders;
    private final CommitProcessFactory commitProcessFactory;
    private final DatabaseTransactionStats transactionMonitor;
    private final ReadReplicaEditionModule editionModule;
    private final AccessCapabilityFactory accessCapabilityFactory;

    public ReadReplicaDatabaseComponents( GlobalModule globalModule, ReadReplicaEditionModule editionModule, DatabaseId databaseId )
    {
        this.editionModule = editionModule;
        this.locksManager = new ReadReplicaLockManager();
        Config globalConfig = globalModule.getGlobalConfig();
        DatabaseLogService databaseLogService = new DatabaseLogService( new DatabaseNameLogContext( databaseId ), globalModule.getLogService() );
        this.statementLocksFactory = new StatementLocksFactorySelector( locksManager, globalConfig, databaseLogService ).select();

        IdContextFactory idContextFactory = IdContextFactoryBuilder.of( globalModule.getFileSystem(), globalModule.getJobScheduler(), globalConfig ).build();

        this.idContext = idContextFactory.createIdContext( databaseId );
        this.tokenHolders = new TokenHolders(
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_PROPERTY_KEY ),
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_LABEL ),
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
        this.commitProcessFactory = ( appender, storageEngine, config ) -> new ReadOnlyTransactionCommitProcess();
        this.transactionMonitor = editionModule.createTransactionMonitor();
        this.accessCapabilityFactory = AccessCapabilityFactory.fixed( new ReadOnly() );
    }

    @Override
    public DatabaseIdContext getIdContext()
    {
        return idContext;
    }

    @Override
    public TokenHolders getTokenHolders()
    {
        return tokenHolders;
    }

    @Override
    public Function<DatabaseLayout,DatabaseLayoutWatcher> getWatcherServiceFactory()
    {
        return editionModule.getWatcherServiceFactory();
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
        return commitProcessFactory;
    }

    @Override
    public TransactionHeaderInformationFactory getHeaderInformationFactory()
    {
        return editionModule.getHeaderInformationFactory();
    }

    @Override
    public Locks getLocks()
    {
        return locksManager;
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
    public QueryEngineProvider getQueryEngineProvider()
    {
        return editionModule.getQueryEngineProvider();
    }

    @Override
    public AccessCapabilityFactory getAccessCapabilityFactory()
    {
        return accessCapabilityFactory;
    }
}
