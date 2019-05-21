/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.catchup.CatchupComponentsFactory;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.dbms.database.MultiDatabaseManager;

import java.io.IOException;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Health;

import static java.lang.String.format;

public abstract class ClusteredMultiDatabaseManager extends MultiDatabaseManager<ClusteredDatabaseContext> implements ClusteredDatabaseManager
{
    protected final ClusteredDatabaseContextFactory contextFactory;
    protected final LogProvider logProvider;
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final Health globalHealths;
    protected final Log log;
    private final Config config;
    protected final StoreFiles storeFiles;
    protected final CatchupComponentsFactory catchupComponentsFactory;

    public ClusteredMultiDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, Log log, CatchupComponentsFactory catchupComponentsFactory,
            FileSystemAbstraction fs, PageCache pageCache, LogProvider logProvider, Config config, Health globalHealths )
    {
        super( globalModule, edition, log );
        this.contextFactory = DefaultClusteredDatabaseContext::new;
        this.logProvider = logProvider;
        this.fs = fs;
        this.log = logProvider.getLog( this.getClass() );
        this.config = config;
        this.pageCache = pageCache;
        this.globalHealths = globalHealths;
        this.catchupComponentsFactory = catchupComponentsFactory;
        this.storeFiles = new StoreFiles( fs, pageCache );
    }

    @Override
    public Optional<ClusteredDatabaseContext> getDatabaseContext( DatabaseId databaseId )
    {
        return Optional.ofNullable( databaseMap.get( databaseId ) );
    }

    protected LogFiles buildTransactionLogs( DatabaseLayout dbLayout )
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

    @Override
    public Health getAllHealthServices()
    {
        return globalHealths;
    }

    @Override
    public <EXCEPTION extends Throwable> void assertHealthy( DatabaseId databaseId, Class<EXCEPTION> cause ) throws EXCEPTION
    {
        getDatabaseHealth( databaseId ).orElseThrow( () ->
                Exceptions.disguiseException( cause,
                        format( "Database %s not found!", databaseId.name() ),
                        new DatabaseNotFoundException( databaseId.name() ) ) )
                .assertHealthy( cause );
    }

    private Optional<Health> getDatabaseHealth( DatabaseId databaseId )
    {
        return Optional.ofNullable( databaseMap.get( databaseId ) ).map( db -> db.database().getDatabaseHealth() );
    }

    @Override
    protected void stopDatabase( DatabaseId databaseId, ClusteredDatabaseContext context )
    {
        try
        {
            super.stopDatabase( databaseId, context );
        }
        catch ( Throwable t )
        {
            throw new DatabaseManagementException( format( "Unable to stop database %s", databaseId ), t );
        }
    }

    @Override
    protected void dropDatabase( DatabaseId databaseId, ClusteredDatabaseContext context )
    {
        try
        {
            // TODO: Should clean up cluster state here for core members.
            super.dropDatabase( databaseId, context );
        }
        catch ( Throwable t )
        {
            throw new DatabaseManagementException( format( "Unable to drop database %s", databaseId ), t );
        }
    }
}
