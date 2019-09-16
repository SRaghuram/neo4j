/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.catchup.CatchupComponentsFactory;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.dbms.database.MultiDatabaseManager;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

public abstract class ClusteredMultiDatabaseManager extends MultiDatabaseManager<ClusteredDatabaseContext>
{
    protected final FileSystemAbstraction fs;
    private final PageCache pageCache;
    protected final ClusteredDatabaseContextFactory contextFactory;
    protected final LogProvider logProvider;
    protected final Log log;
    protected final Config config;
    protected final StoreFiles storeFiles;
    protected final CatchupComponentsFactory catchupComponentsFactory;
    protected final ClusterStateLayout clusterStateLayout;
    private final Set<DatabaseId> shouldRecreateContext;

    public ClusteredMultiDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, CatchupComponentsFactory catchupComponentsFactory,
            FileSystemAbstraction fs, PageCache pageCache, LogProvider logProvider, Config config, ClusterStateLayout clusterStateLayout )
    {
        super( globalModule, edition );
        this.contextFactory = DefaultClusteredDatabaseContext::new;
        this.logProvider = logProvider;
        this.fs = fs;
        this.log = logProvider.getLog( this.getClass() );
        this.config = config;
        this.pageCache = pageCache;
        this.catchupComponentsFactory = catchupComponentsFactory;
        this.shouldRecreateContext = new HashSet<>();
        this.storeFiles = new StoreFiles( fs, pageCache );
        this.clusterStateLayout = clusterStateLayout;
    }

    @Override
    protected final ClusteredDatabaseContext startDatabase( DatabaseId databaseId, ClusteredDatabaseContext context )
    {
        try
        {
            log.info( "Bootstrapping '%s' database.", databaseId.name() );
            if ( shouldRecreateContext.contains( databaseId ) )
            {
                // Clustering components cannot be reused, so we have to create a new context on each cycle.
                context = createDatabaseContext( databaseId );
            }
            shouldRecreateContext.add( databaseId );
            context.clusteredDatabaseLife().init();
            log.info( "Starting '%s' database.", databaseId.name() );
            context.clusteredDatabaseLife().start();
            return context;
        }
        catch ( Throwable t )
        {
            throw new DatabaseManagementException( format( "Unable to start database `%s`", databaseId ), t );
        }
    }

    @Override
    protected final ClusteredDatabaseContext stopDatabase( DatabaseId databaseId, ClusteredDatabaseContext context )
    {
        try
        {
            log.info( "Stopping '%s' database.", databaseId.name() );
            context.clusteredDatabaseLife().stop();
            context.clusteredDatabaseLife().shutdown();
            return context;
        }
        catch ( Throwable t )
        {
            throw new DatabaseManagementException( format( "Unable to stop database `%s`", databaseId ), t );
        }
    }

    public void stopDatabaseBeforeStoreCopy( DatabaseId databaseId )
    {
        forSingleDatabase( databaseId, ( id, context ) ->
        {
            try
            {
                log.info( "Stopping '%s' database for store copy.", databaseId.name() );
                context.database().stop();
                return context;
            }
            catch ( Throwable t )
            {
                throw new DatabaseManagementException( format( "Unable to stop database '%s' for store copy.", databaseId.name() ), t );
            }
        } );
    }

    public void startDatabaseAfterStoreCopy( DatabaseId databaseId )
    {
        forSingleDatabase( databaseId, ( id, context ) ->
        {
            try
            {
                log.info( "Starting '%s' database after store copy.", databaseId.name() );
                context.database().start();
                return context;
            }
            catch ( Throwable t )
            {
                throw new DatabaseManagementException( format( "Unable to start database '%s' after store copy.", databaseId.name() ), t );
            }
        } );
    }

    public abstract void cleanupClusterState( String databaseName );

    protected final LogFiles buildTransactionLogs( DatabaseLayout dbLayout )
    {
        try
        {
            return LogFilesBuilder.activeFilesBuilder( dbLayout, fs, pageCache ).withConfig( config ).build();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
