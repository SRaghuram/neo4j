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
import org.neo4j.monitoring.Health;

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
    public Health getAllHealthServices()
    {
        return globalHealths;
    }

    @Override
    protected final void startDatabase( DatabaseId databaseId, ClusteredDatabaseContext context )
    {
        try
        {
            log.info( "Starting '%s' database.", databaseId.name() );
            context.clusterDatabaseLife().start();
        }
        catch ( Throwable t )
        {
            throw new DatabaseManagementException( "Unable to start database " + databaseId.name(), t );
        }
    }

    @Override
    protected final void stopDatabase( DatabaseId databaseId, ClusteredDatabaseContext context )
    {
        try
        {
            log.info( "Stopping '%s' database.", databaseId.name() );
            context.clusterDatabaseLife().shutdown();
        }
        catch ( Throwable t )
        {
            throw new DatabaseManagementException( "Unable to stop database " + databaseId.name(), t );
        }
    }

    @Override
    protected void dropDatabase( DatabaseId databaseId, ClusteredDatabaseContext context )
    {
        // TODO: Should clean up cluster state here for core members.
        throw new UnsupportedOperationException();
    }

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
