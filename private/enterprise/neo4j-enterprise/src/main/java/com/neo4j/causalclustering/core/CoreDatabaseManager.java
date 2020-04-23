/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.catchup.CatchupComponentsFactory;
import com.neo4j.causalclustering.common.ClusterMonitors;
import com.neo4j.causalclustering.core.state.BootstrapContext;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.causalclustering.core.state.CoreEditionKernelComponents;
import com.neo4j.causalclustering.core.state.CoreKernelResolvers;
import com.neo4j.causalclustering.core.state.snapshot.StoreDownloadContext;
import com.neo4j.dbms.database.ClusteredDatabaseContext;
import com.neo4j.dbms.database.ClusteredMultiDatabaseManager;

import java.io.File;
import java.io.IOException;

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
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseCreationContext;
import org.neo4j.kernel.database.DatabaseNameLogContext;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.DatabaseLogService;
import org.neo4j.monitoring.Monitors;

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
        LifeSupport clusterComponents = new LifeSupport();
        Dependencies coreDatabaseDependencies = new Dependencies( globalModule.getGlobalDependencies() );
        DatabaseLogService coreDatabaseLogService = new DatabaseLogService( new DatabaseNameLogContext( namedDatabaseId ), globalModule.getLogService() );
        Monitors coreDatabaseMonitors = ClusterMonitors.create( globalModule.getGlobalMonitors(), coreDatabaseDependencies );

        var pageCacheTracer = globalModule.getTracers().getPageCacheTracer();
        DatabaseLayout databaseLayout = globalModule.getNeo4jLayout().databaseLayout( namedDatabaseId.name() );

        LogFiles transactionLogs = buildTransactionLogs( databaseLayout );

        BootstrapContext bootstrapContext = new BootstrapContext( namedDatabaseId, databaseLayout, storeFiles, transactionLogs );
        CoreRaftContext raftContext = edition.coreDatabaseFactory().createRaftContext( namedDatabaseId, clusterComponents,
                coreDatabaseMonitors, coreDatabaseDependencies, bootstrapContext, coreDatabaseLogService,
                globalModule.getOtherMemoryPool().getPoolMemoryTracker() );

        var databaseConfig = new DatabaseConfig( config, namedDatabaseId );
        var versionContextSupplier = createVersionContextSupplier( databaseConfig );
        var kernelResolvers = new CoreKernelResolvers();
        var kernelContext = edition.coreDatabaseFactory()
                .createKernelComponents( namedDatabaseId, clusterComponents, raftContext, kernelResolvers,
                        coreDatabaseLogService, versionContextSupplier );

        var databaseCreationContext = newDatabaseCreationContext( namedDatabaseId, coreDatabaseDependencies,
                coreDatabaseMonitors, kernelContext, versionContextSupplier, databaseConfig, coreDatabaseLogService );
        var kernelDatabase = new Database( databaseCreationContext );

        var downloadContext = new StoreDownloadContext( kernelDatabase, storeFiles, transactionLogs, internalDbmsOperator(), pageCacheTracer );

        var coreDatabase = edition.coreDatabaseFactory().createDatabase( namedDatabaseId, clusterComponents, coreDatabaseMonitors, coreDatabaseDependencies,
                downloadContext, kernelDatabase, kernelContext, raftContext, internalDbmsOperator() );

        var ctx = contextFactory.create( kernelDatabase, kernelDatabase.getDatabaseFacade(), transactionLogs,
                storeFiles, logProvider, catchupComponentsFactory, coreDatabase, coreDatabaseMonitors, pageCacheTracer );

        kernelResolvers.registerDatabase( ctx.database() );
        return ctx;
    }

    private DatabaseCreationContext newDatabaseCreationContext( NamedDatabaseId namedDatabaseId, Dependencies parentDependencies, Monitors parentMonitors,
            CoreEditionKernelComponents kernelComponents, VersionContextSupplier versionContextSupplier,
            DatabaseConfig databaseConfig, DatabaseLogService databaseLogService )
    {
        Config config = globalModule.getGlobalConfig();
        var coreDatabaseComponents = new CoreDatabaseComponents( config, edition, kernelComponents, databaseLogService );
        var globalProcedures = edition.getGlobalProcedures();
        return new ModularDatabaseCreationContext( namedDatabaseId, globalModule, parentDependencies, parentMonitors,
                coreDatabaseComponents, globalProcedures, versionContextSupplier, databaseConfig, kernelComponents.leaseService() );
    }

    @Override
    protected void dropDatabase( NamedDatabaseId namedDatabaseId, ClusteredDatabaseContext context )
    {
        super.dropDatabase( namedDatabaseId, context );
        cleanupClusterState( namedDatabaseId.name() );
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
        File raftIdState = clusterStateLayout.raftIdStateFile( databaseName );
        File raftIdStateDir = raftIdState.getParentFile();
        File raftGroupDir = clusterStateLayout.raftGroupDir( databaseName );

        if ( !fs.fileExists( raftGroupDir ) )
        {
            return;
        }
        assert raftIdStateDir.getParentFile().equals( raftGroupDir );

        File[] files = fs.listFiles( raftGroupDir, ( ignored, name ) -> !raftIdStateDir.getName().equals( name ) );

        for ( File file : files )
        {
            if ( file.isDirectory() )
            {
                fs.deleteRecursively( file );
            }
            else if ( !fs.deleteFile( file ) )
            {
                throw new IOException( format( "Unable to delete file %s when dropping database %s", file.getAbsolutePath(), databaseName ) );
            }
        }

        FileUtils.tryForceDirectory( raftGroupDir );

        if ( files.length != 0 && !fs.fileExists( raftIdState ) )
        {
            log.warn( format( "The cluster state directory for %s is non-empty but does not contain a raftId. " +
                    "Cleanup has still been attempted.", databaseName ) );
        }

        if ( !fs.deleteFile( raftIdState ) )
        {
            throw new IOException( format( "Unable to delete file %s when dropping database %s", raftIdState.getAbsolutePath(), databaseName ) );
        }

        fs.deleteRecursively( raftGroupDir );
    }
}
