/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.catchup.CatchupComponentsFactory;
import com.neo4j.causalclustering.common.RaftMonitors;
import com.neo4j.causalclustering.core.state.BootstrapContext;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.causalclustering.core.state.CoreEditionKernelComponents;
import com.neo4j.causalclustering.core.state.CoreKernelResolvers;
import com.neo4j.causalclustering.core.state.snapshot.StoreDownloadContext;
import com.neo4j.dbms.database.ClusteredDatabaseContext;
import com.neo4j.dbms.database.ClusteredMultiDatabaseManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.database.DatabaseConfig;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.ModularDatabaseCreationContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseCreationContext;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.DatabaseLogService;
import org.neo4j.monitoring.Monitors;

import static com.neo4j.dbms.database.DefaultClusteredDatabaseContext.core;
import static java.lang.String.format;

public final class CoreDatabaseManager extends ClusteredMultiDatabaseManager
{
    protected final CoreEditionModule edition;

    CoreDatabaseManager( GlobalModule globalModule, CoreEditionModule edition, CatchupComponentsFactory catchupComponentsFactory, FileSystemAbstraction fs,
            PageCache pageCache, LogProvider logProvider, Config config, ClusterStateLayout clusterStateLayout )
    {
        super( globalModule, edition, catchupComponentsFactory, fs, pageCache, logProvider, config, clusterStateLayout );
        this.edition = edition;
    }

    @Override
    protected ClusteredDatabaseContext createDatabaseContext( NamedDatabaseId namedDatabaseId )
    {
        LifeSupport raftComponents = new LifeSupport();

        Dependencies coreDatabaseDependencies = new Dependencies( globalModule.getGlobalDependencies() );
        DatabaseLogService coreDatabaseLogService = new DatabaseLogService( namedDatabaseId, globalModule.getLogService() );
        Monitors coreDatabaseMonitors = RaftMonitors.create( globalModule.getGlobalMonitors(), coreDatabaseDependencies );

        var pageCacheTracer = globalModule.getTracers().getPageCacheTracer();
        DatabaseLayout databaseLayout = globalModule.getNeo4jLayout().databaseLayout( namedDatabaseId.name() );

        LogFiles transactionLogs = buildTransactionLogs( databaseLayout );

        BootstrapContext bootstrapContext = new BootstrapContext( namedDatabaseId, databaseLayout, storeFiles, transactionLogs );
        CoreRaftContext raftContext = edition.coreDatabaseFactory().createRaftContext( namedDatabaseId, raftComponents, coreDatabaseMonitors,
                coreDatabaseDependencies, bootstrapContext, coreDatabaseLogService, globalModule.getOtherMemoryPool().getPoolMemoryTracker() );

        coreDatabaseDependencies.satisfyDependency( raftContext.replicator());
        var databaseConfig = new DatabaseConfig( config, namedDatabaseId );
        var versionContextSupplier = createVersionContextSupplier( databaseConfig );
        var kernelResolvers = new CoreKernelResolvers();
        var kernelContext = edition.coreDatabaseFactory()
                .createKernelComponents( namedDatabaseId, raftComponents, raftContext, kernelResolvers,
                        coreDatabaseLogService, versionContextSupplier );

        var databaseCreationContext = newDatabaseCreationContext( namedDatabaseId, coreDatabaseDependencies,
                                                                  coreDatabaseMonitors, kernelContext, versionContextSupplier, databaseConfig,
                                                                  coreDatabaseLogService );
        var kernelDatabase = new Database( databaseCreationContext );

        var downloadContext = new StoreDownloadContext( kernelDatabase, storeFiles, transactionLogs, internalDbmsOperator(), pageCacheTracer );

        var coreDatabase = edition.coreDatabaseFactory().createDatabase( namedDatabaseId, raftComponents, coreDatabaseMonitors, coreDatabaseDependencies,
                                                                         downloadContext, kernelDatabase, kernelContext, raftContext, internalDbmsOperator() );

        var ctx = core( kernelDatabase, kernelDatabase.getDatabaseFacade(), transactionLogs,
                        storeFiles, logProvider, catchupComponentsFactory, coreDatabase,
                        coreDatabaseMonitors, raftContext.raftGroup().raftMachine(), pageCacheTracer );

        kernelResolvers.registerDatabase( ctx.database() );
        return ctx;
    }

    private DatabaseCreationContext newDatabaseCreationContext( NamedDatabaseId namedDatabaseId, Dependencies parentDependencies, Monitors parentMonitors,
            CoreEditionKernelComponents kernelComponents, VersionContextSupplier versionContextSupplier,
            DatabaseConfig databaseConfig, DatabaseLogService databaseLogService )
    {
        Config config = globalModule.getGlobalConfig();
        var coreDatabaseComponents = new CoreDatabaseComponents( config, edition, kernelComponents, databaseLogService );
        var globalProcedures = parentDependencies.resolveDependency( GlobalProcedures.class );
        return new ModularDatabaseCreationContext( namedDatabaseId, globalModule, parentDependencies, parentMonitors,
                coreDatabaseComponents, globalProcedures, versionContextSupplier, databaseConfig, kernelComponents.leaseService() );
    }

    @Override
    public void cleanupClusterState( String databaseName )
    {
        try
        {
            deleteCoreStateThenRaftId( databaseName );
        }
        catch ( IOException e )
        {
            throw new DatabaseManagementException( "Was unable to delete cluster state as part of drop. ", e );
        }
    }

    /**
     * When deleting the core state for a database we attempt to delete the raft id for that database last, as
     * in extremis if some other IO failure occurs, the existence of a raft-id indicates that more cleanup may be required.
     */
    private void deleteCoreStateThenRaftId( String databaseName ) throws IOException
    {
        Path raftGroupIdStateFile = clusterStateLayout.raftGroupIdFile( databaseName );
        Path raftGroupIdStateDir = raftGroupIdStateFile.getParent();
        Path raftGroupDir = clusterStateLayout.raftGroupDir( databaseName );

        if ( !fs.fileExists( raftGroupDir ) )
        {
            return;
        }
        assert raftGroupIdStateDir.getParent().equals( raftGroupDir );

        Path[] files = fs.listFiles( raftGroupDir, path -> !raftGroupIdStateDir.getFileName().equals( path.getFileName() ) );

        for ( Path file : files )
        {
            if ( Files.isDirectory( file ) )
            {
                fs.deleteRecursively( file );
            }
            else if ( !fs.deleteFile( file ) )
            {
                throw new IOException( format( "Unable to delete file %s when dropping database %s", file.toAbsolutePath(), databaseName ) );
            }
        }

        FileUtils.tryForceDirectory( raftGroupDir );

        if ( files.length != 0 && !fs.fileExists( raftGroupIdStateFile ) )
        {
            log.warn( format( "The cluster state directory for %s is non-empty but does not contain a raft group ID. " +
                    "Cleanup has still been attempted.", databaseName ) );
        }

        if ( !fs.deleteFile( raftGroupIdStateFile ) )
        {
            throw new IOException( format( "Unable to delete file %s when dropping database %s", raftGroupIdStateFile.toAbsolutePath(), databaseName ) );
        }

        fs.deleteRecursively( raftGroupDir );
    }
}
