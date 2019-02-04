/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.helpers.FakeJobScheduler;
import com.neo4j.causalclustering.identity.StoreId;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;

import org.neo4j.collection.Dependencies;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StubClusteredDatabaseManager implements ClusteredDatabaseManager<ClusteredDatabaseContext>
{
    private SortedMap<String,ClusteredDatabaseContext> databases = new TreeMap<>();
    private boolean isStoppedForSomeReason;

    public StubClusteredDatabaseManager()
    {
    }

    public StubClusteredDatabaseManager( Map<String,? extends ClusteredDatabaseContext> registeredDbs )
    {
        this.databases = new TreeMap<>( registeredDbs );
    }

    @Override
    public void stopForStoreCopy()
    {
        isStoppedForSomeReason = true;
    }

    private boolean globalAvailability()
    {
        return !isStoppedForSomeReason;
    }

    @Override
    public Optional<ClusteredDatabaseContext> getDatabaseContext( String databaseName )
    {
        return Optional.ofNullable( databases.get( databaseName ) );
    }

    @Override
    public StubClusteredDatabaseContext createDatabase( String databaseName )
    {
        throw new UnsupportedOperationException( "Can't register databases directly with the StubClusteredDatabaseManager" );
    }

    public void registerDatabase( String databaseName, ClusteredDatabaseContext db )
    {
        databases.put( databaseName, db );
    }

    public DatabaseContextConfig givenDatabaseWithConfig()
    {
        return new DatabaseContextConfig();
    }

    @Override
    public SortedMap<String,ClusteredDatabaseContext> registeredDatabases()
    {
        return Collections.unmodifiableSortedMap( databases );
    }

    @Override
    public <E extends Throwable> void assertHealthy( String databaseName, Class<E> cause ) throws E
    { //no-op
    }

    @Override
    public DatabaseHealth getAllHealthServices()
    {
        return null;
    }

    //TODO: change lifecycle management to be per database
    @Override
    public void dropDatabase( String databaseName )
    {
    }

    @Override
    public void stopDatabase( String databaseName )
    {
    }

    @Override
    public void startDatabase( String databaseName )
    {
    }

    @Override
    public void init()
    {
    }

    @Override
    public void start()
    {
        isStoppedForSomeReason = false;
    }

    @Override
    public void stop()
    {
        isStoppedForSomeReason = true;
    }

    @Override
    public void shutdown()
    {

    }

    private ClusteredDatabaseContext stubDatabaseFromConfig( DatabaseContextConfig config )
    {
        Database db = mock( Database.class );
        when( db.getDatabaseName() ).thenReturn( config.databaseName );
        when( db.getDatabaseLayout() ).thenReturn( config.databaseLayout );

        StubClusteredDatabaseContext dbContext = new StubClusteredDatabaseContext( db, mock( GraphDatabaseFacade.class ), config.logProvider,
                config.isAvailable, config.monitors );
        if ( config.storeId != null )
        {
            dbContext.setStoreId( config.storeId );
        }
        return dbContext;
    }

    public class DatabaseContextConfig
    {
        private String databaseName;
        private ClusteredDatabaseContext database = mock( ClusteredDatabaseContext.class );
        private DatabaseLayout databaseLayout;
        private LogProvider logProvider = NullLogProvider.getInstance();
        private BooleanSupplier isAvailable = StubClusteredDatabaseManager.this::globalAvailability;
        private Monitors monitors;
        private StoreId storeId;
        private Dependencies dependencies;
        private JobScheduler jobScheduler = new FakeJobScheduler();

        private DatabaseContextConfig()
        {
        }

        public DatabaseContextConfig withDatabaseName( String databaseName )
        {
            this.databaseName = databaseName;
            return this;
        }

        public DatabaseContextConfig withStoreId( StoreId storeId )
        {
            this.storeId = storeId;
            return this;
        }

        public DatabaseContextConfig withDatabaseLayout( DatabaseLayout databaseLayout )
        {
            this.databaseLayout = databaseLayout;
            return this;
        }

        public DatabaseContextConfig withMonitors( Monitors monitors )
        {
            this.monitors = monitors;
            return this;
        }

        public DatabaseContextConfig withJobScheduler( JobScheduler jobScheduler )
        {
            this.jobScheduler = jobScheduler;
            return this;
        }

        public DatabaseContextConfig withLogProvider( LogProvider logProvider )
        {
            this.logProvider = logProvider;
            return this;
        }

        public DatabaseContextConfig withAvailabilitySupplier( BooleanSupplier availabilitySupplier )
        {
            this.isAvailable = availabilitySupplier;
            return this;
        }

        public DatabaseContextConfig withDependencies( Dependencies dependencies )
        {
            this.dependencies = dependencies;
            return this;
        }

        public void register()
        {
            ClusteredDatabaseContext previous = databases.putIfAbsent( databaseName, stubDatabaseFromConfig( this ) );
            if ( previous != null )
            {
                throw new IllegalStateException( "Already had database with name " + databaseName );
            }
        }
    }
}
