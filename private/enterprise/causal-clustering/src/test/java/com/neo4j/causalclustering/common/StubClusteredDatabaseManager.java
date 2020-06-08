/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.catchup.CatchupComponentsFactory;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.dbms.database.ClusteredDatabaseContext;

import java.util.Collections;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import org.neo4j.collection.Dependencies;
import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Health;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.scheduler.CallingThreadJobScheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StubClusteredDatabaseManager extends LifecycleAdapter implements DatabaseManager<ClusteredDatabaseContext>
{
    private SortedMap<NamedDatabaseId,ClusteredDatabaseContext> databases = new TreeMap<>();
    private final TestDatabaseIdRepository databaseIdRepository;

    public StubClusteredDatabaseManager( TestDatabaseIdRepository databaseIdRepository )
    {
        this.databaseIdRepository = databaseIdRepository;
    }

    public StubClusteredDatabaseManager()
    {
        this.databaseIdRepository = new TestDatabaseIdRepository();
    }

    @Override
    public Optional<ClusteredDatabaseContext> getDatabaseContext( NamedDatabaseId namedDatabaseId )
    {
        return Optional.ofNullable( databases.get( namedDatabaseId ) );
    }

    @Override
    public StubClusteredDatabaseContext createDatabase( NamedDatabaseId namedDatabaseId )
    {
        throw new UnsupportedOperationException( "Can't register databases directly with the StubClusteredDatabaseManager" );
    }

    public void registerDatabase( NamedDatabaseId namedDatabaseId, ClusteredDatabaseContext db )
    {
        databases.put( namedDatabaseId, db );
    }

    public DatabaseContextConfig givenDatabaseWithConfig()
    {
        return new DatabaseContextConfig();
    }

    @Override
    public SortedMap<NamedDatabaseId,ClusteredDatabaseContext> registeredDatabases()
    {
        return Collections.unmodifiableSortedMap( databases );
    }

    //TODO: change lifecycle management to be per database
    @Override
    public void dropDatabase( NamedDatabaseId namedDatabaseId )
    {
    }

    @Override
    public void stopDatabase( NamedDatabaseId namedDatabaseId )
    {
    }

    @Override
    public void startDatabase( NamedDatabaseId namedDatabaseId )
    {
    }

    @Override
    public TestDatabaseIdRepository databaseIdRepository()
    {
        return databaseIdRepository;
    }

    private StubClusteredDatabaseContext stubDatabaseFromConfig( DatabaseContextConfig config )
    {
        Database db = mock( Database.class );
        when( db.getNamedDatabaseId() ).thenReturn( config.namedDatabaseId );
        when( db.getDatabaseLayout() ).thenReturn( config.databaseLayout );
        when( db.getDatabaseAvailabilityGuard() ).thenReturn( config.availabilityGuard );
        when( db.getDatabaseHealth() ).thenReturn( config.health );
        when( db.isStarted() ).thenReturn( config.databaseStarted );

        StubClusteredDatabaseContext dbContext = new StubClusteredDatabaseContext( db, mock( GraphDatabaseFacade.class ), config.logFiles, config.storeFiles,
                config.logProvider, config.catchupComponentsFactory, config.leaderLocator );

        if ( config.storeId != null )
        {
            when( db.getStoreId() ).thenReturn( config.storeId );
            dbContext.setStoreId( config.storeId );
        }
        else
        {
            when( db.getStoreId() ).thenReturn( StoreId.UNKNOWN );
        }
        return dbContext;
    }

    public class DatabaseContextConfig
    {
        private NamedDatabaseId namedDatabaseId;
        private DatabaseLayout databaseLayout;
        private LogProvider logProvider = NullLogProvider.getInstance();
        private CatchupComponentsFactory catchupComponentsFactory = dbContext -> mock( CatchupComponentsRepository.CatchupComponents.class );
        private StoreId storeId;
        private Dependencies dependencies;
        private JobScheduler jobScheduler = new CallingThreadJobScheduler();
        private StoreFiles storeFiles = mock( StoreFiles.class );
        private LogFiles logFiles = mock( LogFiles.class );
        private DatabaseAvailabilityGuard availabilityGuard = mock( DatabaseAvailabilityGuard.class );
        private Health health;
        private LeaderLocator leaderLocator;
        private boolean databaseStarted = true;

        private DatabaseContextConfig()
        {
        }

        public DatabaseContextConfig withDatabaseId( NamedDatabaseId namedDatabaseId )
        {
            this.namedDatabaseId = namedDatabaseId;
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

        public DatabaseContextConfig withCatchupComponentsFactory( CatchupComponentsFactory catchupComponentsFactory )
        {
            this.catchupComponentsFactory = catchupComponentsFactory;
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

        public DatabaseContextConfig withDependencies( Dependencies dependencies )
        {
            this.dependencies = dependencies;
            return this;
        }

        public DatabaseContextConfig withStoreFiles( StoreFiles storeFiles )
        {
            this.storeFiles = storeFiles;
            return this;
        }

        public DatabaseContextConfig withLogFiles( LogFiles logFiles )
        {
            this.logFiles = logFiles;
            return this;
        }

        public DatabaseContextConfig withDatabaseAvailabilityGuard( DatabaseAvailabilityGuard availabilityGuard )
        {
            this.availabilityGuard = availabilityGuard;
            return this;
        }

        public DatabaseContextConfig withDatabaseHealth( Health health )
        {
            this.health = health;
            return this;
        }

        public DatabaseContextConfig withStoppedDatabase()
        {
            this.databaseStarted = false;
            return this;
        }

        public DatabaseContextConfig withLeaderLocator( LeaderLocator leaderLocator )
        {
            this.leaderLocator = leaderLocator;
            return this;
        }

        public StubClusteredDatabaseContext register()
        {
            StubClusteredDatabaseContext dbContext = stubDatabaseFromConfig( this );
            ClusteredDatabaseContext previous = databases.putIfAbsent( namedDatabaseId, dbContext );
            if ( previous != null )
            {
                throw new DatabaseExistsException( "Already had database with name " + namedDatabaseId.name() );
            }
            return dbContext;
        }
    }
}
