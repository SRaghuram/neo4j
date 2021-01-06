/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import com.neo4j.causalclustering.catchup.CatchupComponentsFactory;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.dbms.ClusterInternalDbmsOperator;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

public abstract class ClusteredMultiDatabaseManager extends MultiDatabaseManager<ClusteredDatabaseContext>
{
    protected final FileSystemAbstraction fs;
    private final PageCache pageCache;
    protected final LogProvider logProvider;
    protected final Log log;
    protected final Config config;
    protected final StoreFiles storeFiles;
    protected final CatchupComponentsFactory catchupComponentsFactory;
    protected final ClusterStateLayout clusterStateLayout;
    private final ClusterInternalDbmsOperator internalDbmsOperator;

    public ClusteredMultiDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, CatchupComponentsFactory catchupComponentsFactory,
            FileSystemAbstraction fs, PageCache pageCache, LogProvider logProvider, Config config, ClusterStateLayout clusterStateLayout )
    {
        super( globalModule, edition );
        this.internalDbmsOperator = new ClusterInternalDbmsOperator( logProvider );
        this.logProvider = logProvider;
        this.fs = fs;
        this.log = logProvider.getLog( this.getClass() );
        this.config = config;
        this.pageCache = pageCache;
        this.catchupComponentsFactory = catchupComponentsFactory;
        this.storeFiles = new StoreFiles( fs, pageCache );
        this.clusterStateLayout = clusterStateLayout;
    }

    @Override
    protected final void startDatabase( NamedDatabaseId namedDatabaseId, ClusteredDatabaseContext context )
    {
        try
        {
            context = recreateContextIfNeeded( namedDatabaseId, context );
            log.info( "Starting '%s'.", namedDatabaseId );
            context.clusteredDatabase().start();
        }
        catch ( Throwable t )
        {
            throw new DatabaseManagementException( format( "Unable to start database `%s`", namedDatabaseId ), t );
        }
    }

    private ClusteredDatabaseContext recreateContextIfNeeded( NamedDatabaseId namedDatabaseId, ClusteredDatabaseContext context ) throws Exception
    {
        if ( context.clusteredDatabase().hasBeenStarted() )
        {
            context.clusteredDatabase().stop();
            // Clustering components cannot be reused, so we have to create a new context on each start->stop-start cycle.
            var updatedContext = createDatabaseContext( namedDatabaseId );
            databaseMap.put( namedDatabaseId, updatedContext );
            return updatedContext;
        }
        return context;
    }

    @Override
    protected final void stopDatabase( NamedDatabaseId namedDatabaseId, ClusteredDatabaseContext context )
    {
        try
        {
            log.info( "Stopping '%s'.", namedDatabaseId );
            context.clusteredDatabase().stop();
        }
        catch ( Throwable t )
        {
            throw new DatabaseManagementException( format( "An error occurred! Unable to stop database `%s`.", namedDatabaseId ), t );
        }
    }

    @Override
    protected void dropDatabase( NamedDatabaseId namedDatabaseId, ClusteredDatabaseContext context, boolean dumpData )
    {
        try
        {
            super.dropDatabase0( namedDatabaseId, context, dumpData );
            cleanupClusterState( namedDatabaseId.name() );
            removeDatabaseContext( namedDatabaseId );
        }
        catch ( Throwable t )
        {
            throw new DatabaseManagementException( format( "An error occurred! Unable to drop database with name `%s`.", namedDatabaseId.name() ), t );
        }
    }

    public void stopDatabaseBeforeStoreCopy( NamedDatabaseId namedDatabaseId )
    {
        forSingleDatabase( namedDatabaseId, ( id, context ) ->
        {
            try
            {
                log.info( "Stopping '%s' for store copy.", namedDatabaseId );
                context.database().stop();
            }
            catch ( Throwable t )
            {
                throw new DatabaseManagementException( format( "Unable to stop '%s' for store copy.", namedDatabaseId ), t );
            }
        } );
    }

    public void startDatabaseAfterStoreCopy( NamedDatabaseId namedDatabaseId )
    {
        forSingleDatabase( namedDatabaseId, ( id, context ) ->
        {
            try
            {
                log.info( "Starting '%s' after store copy.", namedDatabaseId );
                context.database().start();
            }
            catch ( Throwable t )
            {
                throw new DatabaseManagementException( format( "Unable to start '%s' after store copy.", namedDatabaseId ), t );
            }
        } );
    }

    public abstract void cleanupClusterState( String databaseName );

    protected final LogFiles buildTransactionLogs( DatabaseLayout dbLayout )
    {
        try
        {
            return LogFilesBuilder.activeFilesBuilder( dbLayout, fs, pageCache )
                    .withConfig( config )
                    .withCommandReaderFactory( globalModule.getStorageEngineFactory().commandReaderFactory() )
                    .build();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public final ClusterInternalDbmsOperator internalDbmsOperator()
    {
        return internalDbmsOperator;
    }
}
