/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphdb.factory.module.edition;

import java.io.File;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.common.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.api.security.provider.NoAuthSecurityProvider;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.core.DefaultLabelIdCreator;
import org.neo4j.kernel.impl.core.DefaultPropertyTokenCreator;
import org.neo4j.kernel.impl.core.DefaultRelationshipTypeCreator;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.CanWrite;
import org.neo4j.kernel.impl.factory.CommunityCommitProcessFactory;
import org.neo4j.kernel.impl.factory.ReadOnly;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.LocksFactory;
import org.neo4j.kernel.impl.locking.SimpleStatementLocksFactory;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.LogService;
import org.neo4j.procedure.builtin.routing.CommunityRoutingProcedureInstaller;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.TokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.udc.UsageData;

import static org.neo4j.graphdb.factory.EditionLocksFactories.createLockFactory;
import static org.neo4j.graphdb.factory.EditionLocksFactories.createLockManager;

/**
 * This implementation of {@link AbstractEditionModule} creates the implementations of services
 * that are specific to the Community edition.
 */
public class CommunityEditionModule extends DefaultEditionModule
{
    public static final String COMMUNITY_SECURITY_MODULE_ID = "community-security-module";

    private final Config config;
    private final ConnectorPortRegister portRegister;
    protected final SslPolicyLoader sslPolicyLoader;

    public CommunityEditionModule( GlobalModule globalModule )
    {
        this.config = globalModule.getGlobalConfig();
        this.portRegister = globalModule.getConnectorPortRegister();

        Dependencies globalDependencies = globalModule.getGlobalDependencies();
        Config globalConfig = globalModule.getGlobalConfig();
        LogService logService = globalModule.getLogService();
        FileSystemAbstraction fileSystem = globalModule.getFileSystem();
        PageCache pageCache = globalModule.getPageCache();
        LifeSupport globalLife = globalModule.getGlobalLife();
        SystemNanoClock globalClock = globalModule.getGlobalClock();

        watcherServiceFactory = databaseLayout -> createDatabaseFileSystemWatcher( globalModule.getFileWatcher(), databaseLayout,
                logService, fileWatcherFileNameFilter() );

        this.accessCapability = globalConfig.get( GraphDatabaseSettings.read_only ) ? new ReadOnly() : new CanWrite();

        this.sslPolicyLoader = SslPolicyLoader.create( globalConfig, logService.getInternalLogProvider() );
        globalDependencies.satisfyDependency( sslPolicyLoader ); // for bolt and web server

        LocksFactory lockFactory = createLockFactory( globalConfig, logService );
        locksSupplier = () -> createLockManager( lockFactory, globalConfig, globalClock );
        statementLocksFactoryProvider = locks -> createStatementLocksFactory( locks, globalConfig, logService );

        threadToTransactionBridge = globalDependencies.satisfyDependency( new ThreadToStatementContextBridge() );

        idContextFactory = createIdContextFactory( globalModule, fileSystem );

        tokenHoldersProvider = createTokenHolderProvider( globalModule );

        File kernelContextDirectory = globalModule.getStoreLayout().storeDirectory();
        KernelData kernelData = createKernelData( fileSystem, pageCache, kernelContextDirectory, globalConfig );
        globalDependencies.satisfyDependency( kernelData );
        globalLife.add( kernelData );

        commitProcessFactory = new CommunityCommitProcessFactory();

        headerInformationFactory = createHeaderInformationFactory();

        schemaWriteGuard = createSchemaWriteGuard();

        transactionStartTimeout = globalConfig.get( GraphDatabaseSettings.transaction_start_timeout ).toMillis();

        constraintSemantics = createSchemaRuleVerifier();

        ioLimiter = IOLimiter.UNLIMITED;

        connectionTracker = globalDependencies.satisfyDependency( createConnectionTracker() );

        publishEditionInfo( globalDependencies.resolveDependency( UsageData.class ), globalModule.getDatabaseInfo(), globalConfig );
    }

    protected Function<String,TokenHolders> createTokenHolderProvider( GlobalModule platform )
    {
        Config globalConfig = platform.getGlobalConfig();
        Supplier<Kernel> kernelSupplier = () -> platform.getGlobalDependencies().resolveDependency( DatabaseManager.class )
                        .getDatabaseContext( config.get( GraphDatabaseSettings.default_database ) )
                        .map( DatabaseContext::getDatabase)
                        .map( Database::getKernel )
                        .orElseThrow( () -> new IllegalStateException( "Default database kernel should be always accessible" ) );
        return ignored -> new TokenHolders(
                new DelegatingTokenHolder( createPropertyKeyCreator( globalConfig, kernelSupplier ), TokenHolder.TYPE_PROPERTY_KEY ),
                new DelegatingTokenHolder( createLabelIdCreator( globalConfig, kernelSupplier ), TokenHolder.TYPE_LABEL ),
                new DelegatingTokenHolder( createRelationshipTypeCreator( globalConfig, kernelSupplier ), TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
    }

    protected IdContextFactory createIdContextFactory( GlobalModule globalModule, FileSystemAbstraction fileSystem )
    {
        return IdContextFactoryBuilder.of( fileSystem, globalModule.getJobScheduler() ).build();
    }

    protected Predicate<String> fileWatcherFileNameFilter()
    {
        return communityFileWatcherFileNameFilter();
    }

    static Predicate<String> communityFileWatcherFileNameFilter()
    {
        return fileName -> fileName.startsWith( TransactionLogFilesHelper.DEFAULT_NAME );
    }

    protected ConstraintSemantics createSchemaRuleVerifier()
    {
        return new StandardConstraintSemantics();
    }

    protected StatementLocksFactory createStatementLocksFactory( Locks locks, Config config, LogService logService )
    {
        return new SimpleStatementLocksFactory( locks );
    }

    protected SchemaWriteGuard createSchemaWriteGuard()
    {
        return SchemaWriteGuard.ALLOW_ALL_WRITES;
    }

    protected static TokenCreator createRelationshipTypeCreator( Config config, Supplier<Kernel> kernelSupplier )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            return new DefaultRelationshipTypeCreator( kernelSupplier );
        }
    }

    protected static TokenCreator createPropertyKeyCreator( Config config, Supplier<Kernel> kernelSupplier )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            return new DefaultPropertyTokenCreator( kernelSupplier );
        }
    }

    protected static TokenCreator createLabelIdCreator( Config config, Supplier<Kernel> kernelSupplier )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            return new DefaultLabelIdCreator( kernelSupplier );
        }
    }

    private KernelData createKernelData( FileSystemAbstraction fileSystem, PageCache pageCache, File storeDir, Config config )
    {
        return new KernelData( fileSystem, pageCache, storeDir, config );
    }

    protected TransactionHeaderInformationFactory createHeaderInformationFactory()
    {
        return TransactionHeaderInformationFactory.DEFAULT;
    }

    @Override
    public void registerEditionSpecificProcedures( GlobalProcedures globalProcedures ) throws KernelException
    {
        CommunityRoutingProcedureInstaller routingProcedureInstaller = new CommunityRoutingProcedureInstaller( portRegister, config );
        routingProcedureInstaller.install( globalProcedures );
    }

    @Override
    public void createSecurityModule( GlobalModule globalModule, GlobalProcedures globalProcedures )
    {
        LifeSupport globalLife = globalModule.getGlobalLife();
        if ( globalModule.getGlobalConfig().get( GraphDatabaseSettings.auth_enabled ) )
        {
            SecurityModule securityModule = setupSecurityModule( globalModule, this,
                    globalModule.getLogService().getUserLog( getClass() ), globalProcedures, COMMUNITY_SECURITY_MODULE_ID );
            globalLife.add( securityModule );
            this.securityProvider = securityModule;
        }
        else
        {
            NoAuthSecurityProvider noAuthSecurityProvider = NoAuthSecurityProvider.INSTANCE;
            globalLife.add( noAuthSecurityProvider );
            this.securityProvider = noAuthSecurityProvider;
        }
    }
}
