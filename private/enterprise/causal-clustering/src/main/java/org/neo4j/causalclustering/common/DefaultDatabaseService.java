/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.common;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.StoreLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityRequirement;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

public class DefaultDatabaseService<DB extends LocalDatabase> implements Lifecycle, DatabaseService
{
    public static final String STOPPED_MSG = "Local databases are stopped";
    public static final String COPYING_STORE_MSG = "Local databases are stopped to copy a store from another cluster member";
    private static final AvailabilityRequirement notStoppedReq = () -> STOPPED_MSG;
    private static final AvailabilityRequirement notCopyingReq = () -> COPYING_STORE_MSG;

    private final AvailabilityGuard availabilityGuard;
    private final Supplier<DatabaseHealth> databaseHealthSupplier;
    private final Map<String,DB> databases;
    private final LocalDatabaseFactory<DB> databaseFactory;
    private final LifeSupport databasesLife = new LifeSupport();
    private final Supplier<DatabaseManager> databaseManagerSupplier;
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final StoreLayout storeLayout;
    private final LogProvider logProvider;
    private final Log log;
    private final Config config;

    private volatile DatabaseHealth databaseHealth;
    private volatile AvailabilityRequirement currentRequirement;

    public DefaultDatabaseService( LocalDatabaseFactory<DB> databaseFactory, Supplier<DatabaseManager> databaseManagerSupplier, StoreLayout storeLayout,
            AvailabilityGuard availabilityGuard, Supplier<DatabaseHealth> databaseHealthSupplier, FileSystemAbstraction fs,
            PageCache pageCache, LogProvider logProvider, Config config )
    {

        this.databaseManagerSupplier = databaseManagerSupplier;
        this.availabilityGuard = availabilityGuard;
        this.databaseHealthSupplier = databaseHealthSupplier;
        this.fs = fs;
        this.storeLayout = storeLayout;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( this.getClass() );
        this.config = config;
        this.databases = new LinkedHashMap<>();
        this.databaseFactory = databaseFactory;
        this.pageCache = pageCache;
        raiseAvailabilityGuard( notStoppedReq );
    }

    private void raiseAvailabilityGuard( AvailabilityRequirement requirement )
    {
        // it is possible for a local database to be created and stopped right after that to perform a store copy
        // in this case we need to impose new requirement and drop the old one
        availabilityGuard.require( requirement );
        if ( currentRequirement != null )
        {
            availabilityGuard.fulfill( currentRequirement );
        }
        currentRequirement = requirement;
    }

    private synchronized void stopWithRequirement( AvailabilityRequirement requirement ) throws Throwable
    {
        log.info( "Stopping, reason: " + requirement.description() );
        raiseAvailabilityGuard( requirement );
        databaseHealth = null;
        databasesLife.stop();
        databaseManagerSupplier.get().stop();
    }

    @Override
    public void init() throws Throwable
    {
        databaseManagerSupplier.get().init();
    }

    @Override
    public synchronized void start() throws Throwable
    {
        if ( areAvailable() )
        {
            return;
        }
        databaseManagerSupplier.get().start();
        databasesLife.start();
        availabilityGuard.fulfill( currentRequirement );
        currentRequirement = null;
    }

    @Override
    public void stop() throws Throwable
    {
        stopWithRequirement( notStoppedReq );
    }

    @Override
    public void shutdown() throws Throwable
    {
        databaseManagerSupplier.get().shutdown();
    }

    /**
     * Stop database to perform a store copy. This will raise {@link DatabaseAvailabilityGuard} with
     * a more friendly blocking requirement.
     */
    public void stopForStoreCopy() throws Throwable
    {
        stopWithRequirement( notCopyingReq );
    }

    public boolean areAvailable()
    {
       return currentRequirement == null;
    }

    public Optional<DB> get( String databaseName )
    {
        return Optional.ofNullable( databases.get( databaseName ) );
    }

    public DB registerDatabase( String databaseName )
    {
        DB database = createDatabase( databaseName );
        databases.put( databaseName, database );
        databasesLife.add( database );
        return database;
    }

    protected DB createDatabase( String databaseName )
    {
        StoreFiles storeFiles = new StoreFiles( fs, pageCache );
        DatabaseLayout dbLayout = storeLayout.databaseLayout( databaseName );
        LogFiles logFiles = buildLocalDatabaseLogFiles( dbLayout );
        return databaseFactory.create( databaseName, databaseManagerSupplier, dbLayout, logFiles, storeFiles, logProvider, this::areAvailable );
    }

    public Map<String,DB> registeredDatabases()
    {
        return Collections.unmodifiableMap( databases );
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

    public <EXCEPTION extends Throwable> void assertHealthy( Class<EXCEPTION> cause ) throws EXCEPTION
    {
        getDatabaseHealth().assertHealthy( cause );
    }

    private DatabaseHealth getDatabaseHealth()
    {
        if ( databaseHealth == null )
        {
            databaseHealth = databaseHealthSupplier.get();
        }
        return databaseHealth;
    }

}
