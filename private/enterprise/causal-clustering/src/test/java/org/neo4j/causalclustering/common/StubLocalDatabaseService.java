/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;

import org.neo4j.causalclustering.helpers.FakeJobScheduler;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;

import static org.mockito.Mockito.mock;

public class StubLocalDatabaseService implements DatabaseService
{
    private Map<String,LocalDatabase> databases = new HashMap<>();
    private boolean isStoppedForSomeReason;
    private DatabaseHealth dbHealth = new DatabaseHealth( mock( DatabasePanicEventGenerator.class ), NullLog.getInstance() );
    private DatabaseManager databaseManager = mock( DatabaseManager.class );

    public StubLocalDatabaseService()
    {
    }

    public StubLocalDatabaseService( Map<String,LocalDatabase> registeredDbs )
    {
        this.databases = registeredDbs;
    }

    @Override
    public void stopForStoreCopy()
    {
        isStoppedForSomeReason = true;
    }

    @Override
    public boolean areAvailable()
    {
        return !isStoppedForSomeReason;
    }

    @Override
    public Optional<LocalDatabase> get( String databaseName )
    {
        return Optional.ofNullable( databases.get( databaseName ) );
    }

    @Override
    public LocalDatabase registerDatabase( String databaseName )
    {
        throw new UnsupportedOperationException( "Can't register databases directly with the StubLocalDatabaseService" );
    }

    public void registerDatabase( String databaseName, LocalDatabase db )
    {
        databases.put( databaseName, db );
    }

    public LocalDatabaseConfig givenDatabaseWithConfig()
    {
        return new LocalDatabaseConfig();
    }

    public void registerAllDatabases( Map<String,LocalDatabase> registeredDbs )
    {
        this.databases = registeredDbs;
    }

    @Override
    public Map<String,LocalDatabase> registeredDatabases()
    {
        return databases;
    }

    @Override
    public <E extends Throwable> void assertHealthy( Class<E> cause ) throws E
    {
        dbHealth.assertHealthy( cause );
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

    private LocalDatabase stubDatabaseFromConfig( LocalDatabaseConfig config )
    {
        return new StubLocalDatabase( config.databaseName, () -> config.databaseManager, config.databaseLayout,
                config.logProvider, config.isAvailable, config.monitors );
    }

    public class LocalDatabaseConfig
    {
        private String databaseName;
        private DatabaseManager databaseManager = StubLocalDatabaseService.this.databaseManager;
        private DatabaseLayout databaseLayout;
        private LogProvider logProvider = NullLogProvider.getInstance();
        private BooleanSupplier isAvailable = StubLocalDatabaseService.this::areAvailable;
        private Monitors monitors;
        private Dependencies dependencies;
        private JobScheduler jobScheduler = new FakeJobScheduler();

        private LocalDatabaseConfig()
        {
        }

        public LocalDatabaseConfig withDatabaseName( String databaseName )
        {
            this.databaseName = databaseName;
            return this;
        }

        public LocalDatabaseConfig withDatabaseLayout( DatabaseLayout databaseLayout )
        {
            this.databaseLayout = databaseLayout;
            return this;
        }

        public LocalDatabaseConfig withMonitors( Monitors monitors )
        {
            this.monitors = monitors;
            return this;
        }

        public LocalDatabaseConfig withJobScheduler( JobScheduler jobScheduler )
        {
            this.jobScheduler = jobScheduler;
            return this;
        }

        public LocalDatabaseConfig withDatabaseManager( DatabaseManager databaseManager )
        {
            this.databaseManager = databaseManager;
            return this;
        }

        public LocalDatabaseConfig withLogProvider( LogProvider logProvider )
        {
            this.logProvider = logProvider;
            return this;
        }

        public LocalDatabaseConfig withAvailabilitySupplier( BooleanSupplier availabilitySupplier )
        {
            this.isAvailable = availabilitySupplier;
            return this;
        }

        public LocalDatabaseConfig withDependencies( Dependencies dependencies )
        {
            this.dependencies = dependencies;
            return this;
        }

        public void register()
        {
            LocalDatabase previous = databases.putIfAbsent( databaseName, stubDatabaseFromConfig( this ) );
            if ( previous != null )
            {
                throw new IllegalStateException( "Already had database with name " + databaseName );
            }
        }
    }

    public interface NeedsDatabaseLayout
    {
        LocalDatabaseConfig withDatabaseLayout( DatabaseLayout databaseLayout );
    }
}
