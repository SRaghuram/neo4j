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
import org.neo4j.dbms.database.DatabaseManagementException;
import org.neo4j.dbms.database.DatabaseNotFoundException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.helpers.Exceptions;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityRequirement;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Health;
import org.neo4j.util.VisibleForTesting;

import static java.lang.String.format;

public class ClusteredMultiDatabaseManager extends MultiDatabaseManager<ClusteredDatabaseContext> implements ClusteredDatabaseManager
{
    public static final String STOPPED_MSG = "Local databases are stopped";
    public static final String COPYING_STORE_MSG = "Local databases are stopped to copy a store from another cluster member";
    private static final AvailabilityRequirement notStoppedReq = () -> STOPPED_MSG;
    private static final AvailabilityRequirement notCopyingReq = () -> COPYING_STORE_MSG;

    private final ClusteredDatabaseContextFactory contextFactory;
    private final LogProvider logProvider;
    private final AvailabilityGuard availabilityGuard;
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final Health globalHealths;
    private final Log log;
    private final Config config;
    private final StoreFiles storeFiles;
    private final CatchupComponentsFactory catchupComponentsFactory;

    private volatile AvailabilityRequirement currentRequirement;

    public ClusteredMultiDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, Log log, GraphDatabaseFacade facade,
            CatchupComponentsFactory catchupComponentsFactory, FileSystemAbstraction fs,
            PageCache pageCache, LogProvider logProvider, Config config, Health globalHealths, AvailabilityGuard availabilityGuard )
    {
        this( globalModule, edition, log, facade, DefaultClusteredDatabaseContext::new, catchupComponentsFactory, fs, pageCache,
                logProvider, config, globalHealths, availabilityGuard );
    }

    @VisibleForTesting // allows to inject a ClusteredDatabaseContextFactory
    public ClusteredMultiDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, Log log, GraphDatabaseFacade facade,
            ClusteredDatabaseContextFactory contextFactory, CatchupComponentsFactory catchupComponentsFactory, FileSystemAbstraction fs,
            PageCache pageCache, LogProvider logProvider, Config config, Health globalHealths, AvailabilityGuard availabilityGuard )
    {

        super( globalModule, edition, log, facade );
        this.contextFactory = contextFactory;
        this.logProvider = logProvider;
        this.availabilityGuard = availabilityGuard;
        this.fs = fs;
        this.log = logProvider.getLog( this.getClass() );
        this.config = config;
        this.pageCache = pageCache;
        this.globalHealths = globalHealths;
        this.catchupComponentsFactory = catchupComponentsFactory;
        this.storeFiles = new StoreFiles( fs, pageCache );
        raiseAvailabilityGuard( notStoppedReq );
    }

    private void raiseAvailabilityGuard( AvailabilityRequirement requirement )
    {
        // it is possible for a local database to be created and stopped right after that to perform a store copy
        // in this case we need to impose new requirement and drop the old one
        availabilityGuard.require( requirement );
        if ( currentRequirement != null )
        {
            dropAvailabilityGuard();
        }
        currentRequirement = requirement;
    }

    private void dropAvailabilityGuard()
    {
        availabilityGuard.fulfill( currentRequirement );
        currentRequirement = null;
    }

    private synchronized void stopWithRequirement( AvailabilityRequirement requirement )
    {
        log.info( "Stopping, reason: " + requirement.description() );
        boolean storeCopying = requirement == notCopyingReq;
        raiseAvailabilityGuard( requirement );
        if ( started )
        {
            started = false;
            forEachDatabase( ( name, db ) -> stopDatabase( name, db, storeCopying ), true );
        }
    }

    @Override
    public synchronized void start()
    {
        if ( isAvailable() )
        {
            return;
        }
        started = true;
        startAllDatabases();
        dropAvailabilityGuard();
    }

    private boolean isAvailable()
    {
        return currentRequirement == null;
    }

    @Override
    public void stop() throws Exception
    {
        stopWithRequirement( notStoppedReq );
    }

    /**
     * Stop database to perform a store copy. This will raise {@link DatabaseAvailabilityGuard} with
     * a more friendly blocking requirement.
     */
    public void stopForStoreCopy() throws Throwable
    {
        stopWithRequirement( notCopyingReq );
    }

    @Override
    public Optional<ClusteredDatabaseContext> getDatabaseContext( DatabaseId databaseId )
    {
        return Optional.ofNullable( databaseMap.get( databaseId ) );
    }

    protected ClusteredDatabaseContext createNewDatabaseContext( DatabaseId databaseId )
    {
        return super.createNewDatabaseContext( databaseId );
    }

    @Override
    protected ClusteredDatabaseContext createDatabaseContext( Database database, GraphDatabaseFacade facade )
    {
        LogFiles transactionLogs = buildTransactionLogs( database.getDatabaseLayout() );
        return contextFactory.create( database, facade, transactionLogs, storeFiles, logProvider, catchupComponentsFactory );
    }

    private LogFiles buildTransactionLogs( DatabaseLayout dbLayout )
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

    public Health getAllHealthServices()
    {
        return globalHealths;
    }

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

    /*
        Must start and stop clustered contexts explicitly, as start/stop on the parent class only passes through to the Database object,
        whilst our contexts are themselves lifecycles. Context's must be stopped *before* the underlying database, and started *after*.
     */
    @Override
    protected void startDatabase( DatabaseId databaseId, ClusteredDatabaseContext context )
    {
        try
        {
            super.startDatabase( databaseId, context );
        }
        catch ( Throwable t )
        {
            throw new DatabaseManagementException( format( "Unable to start database %s", databaseId ), t );
        }
    }

    @Override
    protected void stopDatabase( DatabaseId databaseId, ClusteredDatabaseContext context )
    {
        stopDatabase( databaseId, context, false );
    }

    protected void stopDatabase( DatabaseId databaseId, ClusteredDatabaseContext context, boolean storeCopying )
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
            //TODO: Should clean up cluster state here for core members
            super.dropDatabase( databaseId, context );
        }
        catch ( Throwable t )
        {
            throw new DatabaseManagementException( format( "Unable to drop database %s", databaseId ), t );
        }
    }
}
