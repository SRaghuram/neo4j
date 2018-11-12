/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.readreplica;

import java.io.File;
import java.util.function.Function;

import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseContext;
import org.neo4j.graphdb.factory.module.id.DatabaseIdContext;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.ReadOnlyTransactionCommitProcess;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.core.DelegatingTokenHolder;
import org.neo4j.kernel.impl.core.ReadOnlyTokenCreator;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.enterprise.id.EnterpriseIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.factory.StatementLocksFactorySelector;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.util.watcher.FileSystemWatcherService;
import org.neo4j.logging.internal.LogService;
import org.neo4j.time.SystemNanoClock;

public class ReadReplicaDatabaseContext implements EditionDatabaseContext
{
    private final String databaseName;
    private final Locks locksManager;
    private final StatementLocksFactory statementLocksFactory;
    private final DatabaseIdContext idContext;
    private final TokenHolders tokenHolders;
    private final CommitProcessFactory commitProcessFactory;
    private final DatabaseTransactionStats transactionMonitor;
    private final EnterpriseReadReplicaEditionModule editionModule;

    public ReadReplicaDatabaseContext( PlatformModule platformModule, EnterpriseReadReplicaEditionModule editionModule, String databaseName )
    {
        this.editionModule = editionModule;
        this.databaseName = databaseName;
        this.locksManager = new ReadReplicaLockManager();
        this.statementLocksFactory = new StatementLocksFactorySelector( locksManager, platformModule.config, platformModule.logService ).select();

        IdContextFactory idContextFactory =
                IdContextFactoryBuilder.of( new EnterpriseIdTypeConfigurationProvider( platformModule.config ), platformModule.jobScheduler )
                .withFileSystem( platformModule.fileSystem )
                .build();

        this.idContext = idContextFactory.createIdContext( databaseName );
        this.tokenHolders = new TokenHolders(
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_PROPERTY_KEY ),
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_LABEL ),
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
        this.commitProcessFactory = ( appender, storageEngine, config ) -> new ReadOnlyTransactionCommitProcess();
        this.transactionMonitor = editionModule.createTransactionMonitor();
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
    public Function<File,FileSystemWatcherService> getWatcherServiceFactory()
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
        return commitProcessFactory;
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
    public DatabaseAvailabilityGuard createDatabaseAvailabilityGuard( SystemNanoClock clock, LogService logService, Config config )
    {
        return editionModule.createDatabaseAvailabilityGuard( databaseName, clock, logService, config );
    }
}
