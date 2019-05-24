/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.catchup.CatchupComponentsFactory;
import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.common.ClusteredMultiDatabaseManager;
import com.neo4j.causalclustering.core.state.BootstrapContext;
import com.neo4j.causalclustering.core.state.CoreEditionKernelComponents;
import com.neo4j.causalclustering.core.state.CoreKernelResolvers;
import com.neo4j.causalclustering.core.state.snapshot.StoreDownloadContext;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.ModularDatabaseCreationContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseCreationContext;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseNameLogContext;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.DatabaseLogService;
import org.neo4j.monitoring.Health;
import org.neo4j.monitoring.Monitors;

import static java.lang.String.format;

public class CoreDatabaseManager extends ClusteredMultiDatabaseManager
{
    protected final CoreEditionModule edition;

    CoreDatabaseManager( GlobalModule globalModule, CoreEditionModule edition, Log log, CatchupComponentsFactory catchupComponentsFactory,
            FileSystemAbstraction fs, PageCache pageCache, LogProvider logProvider, Config config, Health globalHealths )
    {
        super( globalModule, edition, log, catchupComponentsFactory, fs, pageCache, logProvider, config, globalHealths );
        this.edition = edition;
    }

    @Override
    protected ClusteredDatabaseContext createDatabaseContext( DatabaseId databaseId )
    {
        LifeSupport coreDatabaseLife = new LifeSupport();
        Monitors coreDatabaseMonitors = new Monitors( globalModule.getGlobalMonitors() );
        Dependencies coreDatabaseDependencies = new Dependencies( globalModule.getGlobalDependencies() );
        DatabaseLogService coreDatabaseLogService = new DatabaseLogService( new DatabaseNameLogContext( databaseId ), globalModule.getLogService() );

        DatabaseLayout databaseLayout = globalModule.getStoreLayout().databaseLayout( databaseId.name() );

        LogFiles transactionLogs = buildTransactionLogs( databaseLayout );

        BootstrapContext bootstrapContext = new BootstrapContext( databaseId, databaseLayout, storeFiles, transactionLogs );
        CoreRaftContext raftContext = edition.coreDatabaseFactory().createRaftContext(
                databaseId, coreDatabaseLife, coreDatabaseMonitors, coreDatabaseDependencies, bootstrapContext, coreDatabaseLogService );

        CoreKernelResolvers kernelResolvers = new CoreKernelResolvers();
        CoreEditionKernelComponents kernelContext = edition.coreDatabaseFactory().createKernelComponents(
                databaseId, coreDatabaseLife, raftContext, kernelResolvers, coreDatabaseLogService );

        log.info( "Creating '%s' database.", databaseId.name() );
        DatabaseCreationContext databaseCreationContext = newDatabaseCreationContext( databaseId, kernelContext, coreDatabaseDependencies,
                coreDatabaseMonitors, coreDatabaseLogService );
        Database kernelDatabase = new Database( databaseCreationContext );

        // TODO: Merge/change these contexts into something better? Perhaps a ReplicatedDatabaseContext again?
        StoreDownloadContext downloadContext = new StoreDownloadContext( kernelDatabase, storeFiles, transactionLogs );

        edition.coreDatabaseFactory().createDatabase( databaseId, coreDatabaseLife, coreDatabaseMonitors, coreDatabaseDependencies, downloadContext,
                kernelDatabase, kernelContext, raftContext );

        var ctx = contextFactory.create( kernelDatabase, kernelDatabase.getDatabaseFacade(), transactionLogs, storeFiles, logProvider, catchupComponentsFactory,
                coreDatabaseLife, coreDatabaseMonitors );

        kernelResolvers.registerDatabase( ctx.database() );
        return ctx;
    }

    private DatabaseCreationContext newDatabaseCreationContext( DatabaseId databaseId, CoreEditionKernelComponents kernelComponents,
            Dependencies parentDependencies, Monitors parentMonitors, DatabaseLogService databaseLogService )
    {
        Config config = globalModule.getGlobalConfig();
        CoreDatabaseComponents coreDatabaseComponents = new CoreDatabaseComponents( config, edition, kernelComponents, databaseLogService );
        GlobalProcedures globalProcedures = edition.getGlobalProcedures();
        return new ModularDatabaseCreationContext( databaseId, globalModule, parentDependencies, parentMonitors, coreDatabaseComponents, globalProcedures );
    }

    @Override
    public ClusteredDatabaseContext createDatabase( DatabaseId databaseId ) throws DatabaseExistsException
    {
        return databaseMap.compute( databaseId, ( key, currentContext ) ->
        {
            if ( currentContext != null )
            {
                throw new DatabaseExistsException( format( "Database with name `%s` already exists.", databaseId.name() ) );
            }
            ClusteredDatabaseContext databaseContext = createDatabaseContext( databaseId );
            if ( started )
            {
                databaseContext.clusterDatabaseLife().start();
            }
            return databaseContext;
        } );
    }

    @Override
    public void startDatabase( DatabaseId databaseId ) throws DatabaseNotFoundException
    {
        databaseMap.compute( databaseId, ( key, currentContext ) ->
        {
            if ( currentContext == null )
            {
                throw new DatabaseNotFoundException( format( "Database with name `%s` not found.", databaseId.name() ) );
            }
            startDatabase( databaseId, currentContext );
            currentContext.clusterDatabaseLife().start();
            return currentContext;
        } );
    }

    @Override
    public void stopDatabase( DatabaseId databaseId ) throws DatabaseNotFoundException
    {
        databaseMap.compute( databaseId, ( key, currentContext ) ->
        {
            if ( currentContext == null )
            {
                throw new DatabaseNotFoundException( format( "Database with name `%s` not found.", databaseId.name() ) );
            }
            currentContext.clusterDatabaseLife().stop();
            stopDatabase( databaseId, currentContext );
            return currentContext;
        } );
    }

    @Override
    public void dropDatabase( DatabaseId databaseId ) throws DatabaseNotFoundException
    {
        throw new UnsupportedOperationException( "Not implemented" );
//        databaseMap.compute( databaseId, ( key, currentContext ) ->
//        {
//            if ( currentContext == null )
//            {
//                throw new DatabaseNotFoundException( format( "Database with name `%s` not found.", databaseId.name() ) );
//            }
//            dropDatabase( databaseId, currentContext );
//            return null;
//        } );
    }

    @Override
    protected void startDatabase( DatabaseId databaseId, ClusteredDatabaseContext context )
    {
        try
        {
            log.info( "Starting '%s' database.", databaseId.name() );
            context.clusterDatabaseLife().start();
        }
        catch ( Throwable t )
        {
            throw new DatabaseManagementException( format( "Unable to start database %s", databaseId ), t );
        }
    }

    @Override
    protected void stopDatabase( DatabaseId databaseId, ClusteredDatabaseContext context )
    {
        try
        {
            log.info( "Stopping '%s' database.", databaseId.name() );
            context.clusterDatabaseLife().stop();
        }
        catch ( Throwable t )
        {
            throw new DatabaseManagementException( format( "Unable to stop database %s", databaseId ), t );
        }
    }

    @Override
    protected void dropDatabase( DatabaseId databaseId, ClusteredDatabaseContext context )
    {
        throw new UnsupportedOperationException();
    }
}
