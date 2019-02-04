/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.dbms.database.MultiDatabaseManager;

import java.io.IOException;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseNotFoundException;
import org.neo4j.dbms.database.UnableToStartDatabaseException;
import org.neo4j.dbms.database.UnableToStopDatabaseException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityRequirement;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.monitoring.DatabaseHealth;

import static java.lang.String.format;

public class ClusteredMultiDatabaseManager<DB extends ClusteredDatabaseContext> extends MultiDatabaseManager<DB>
        implements Lifecycle, ClusteredDatabaseManager<DB>
{
    public static final String STOPPED_MSG = "Local databases are stopped";
    public static final String COPYING_STORE_MSG = "Local databases are stopped to copy a store from another cluster member";
    private static final AvailabilityRequirement notStoppedReq = () -> STOPPED_MSG;
    private static final AvailabilityRequirement notCopyingReq = () -> COPYING_STORE_MSG;

    private final ClusteredDatabaseContextFactory<DB> contextFactory;
    private final LogProvider logProvider;
    private final AvailabilityGuard availabilityGuard;
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final DatabaseHealth globalHealths;
    private final Log log;
    private final Config config;
    private final StoreFiles storeFiles;

    private volatile AvailabilityRequirement currentRequirement;

    public ClusteredMultiDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, Logger log, GraphDatabaseFacade facade,
            ClusteredDatabaseContextFactory<DB> contextFactory, FileSystemAbstraction fs, PageCache pageCache, LogProvider logProvider, Config config,
            DatabaseHealth globalHealths, AvailabilityGuard availabilityGuard )
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
        raiseAvailabilityGuard( notStoppedReq );
        storeFiles = new StoreFiles( fs, pageCache );
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

    private synchronized void stopWithRequirement( AvailabilityRequirement requirement ) throws Exception
    {
        log.info( "Stopping, reason: " + requirement.description() );
        raiseAvailabilityGuard( requirement );
        if ( started )
        {
            started = false;
            forEachDatabase( this::stopDatabase );
        }
    }

    @Override
    public synchronized void start() throws Exception
    {
        if ( isAvailable() )
        {
            return;
        }
        started = true;
        forEachDatabase( this::startDatabase );
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
    public Optional<DB> getDatabaseContext( String databaseName )
    {
        return Optional.ofNullable( databaseMap.get( databaseName ) );
    }

    @Override
    protected DB createNewDatabaseContext( String databaseName )
    {
        return super.createNewDatabaseContext( databaseName );
    }

    @Override
    protected DB databaseContextFactory( Database database, GraphDatabaseFacade facade )
    {
        LogFiles logFiles = buildLocalDatabaseLogFiles( database.getDatabaseLayout() );
        return contextFactory.create( database, facade, logFiles, storeFiles, logProvider, this::isAvailable );
    }

    private LogFiles buildLocalDatabaseLogFiles( DatabaseLayout dbLayout )
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

    public DatabaseHealth getAllHealthServices()
    {
        return globalHealths;
    }

    public <EXCEPTION extends Throwable> void assertHealthy( String databaseName, Class<EXCEPTION> cause ) throws EXCEPTION
    {
        getDatabaseHealth( databaseName ).orElseThrow( () ->
                DatabaseHealth.constructPanicDisguise( cause,
                        format( "Database %s not found!", databaseName ),
                        new DatabaseNotFoundException( databaseName ) ) )
                .assertHealthy( cause );
    }

    private Optional<DatabaseHealth> getDatabaseHealth( String databaseName )
    {
        return Optional.ofNullable( databaseMap.get( databaseName ) ).map( db -> db.database().getDatabaseHealth() );
    }

    /*
        Must start and stop clustered contexts explicitly, as start/stop on the parent class only passes through to the Database object,
        whilst our contexts are themselves lifecycles. Context's must be stopped *before* the underlying database, and started *after*.
     */
    @Override
    protected void startDatabase( String databaseName, DB context )
    {
        try
        {
            super.startDatabase( databaseName, context );
            context.start();
        }
        catch ( Throwable t )
        {
            throw new UnableToStartDatabaseException( t );
        }
    }

    @Override
    protected void stopDatabase( String databaseName, DB context )
    {
        try
        {
            context.stop();
            super.stopDatabase( databaseName, context );
        }
        catch ( Throwable t )
        {
            throw new UnableToStopDatabaseException( t );
        }
    }

    @Override
    protected void dropDatabase( String databaseName, DB context )
    {
        try
        {
            context.stop();
            super.dropDatabase( databaseName, context );
        }
        catch ( Throwable t )
        {
            throw new UnableToStopDatabaseException( t );
        }
    }
}
