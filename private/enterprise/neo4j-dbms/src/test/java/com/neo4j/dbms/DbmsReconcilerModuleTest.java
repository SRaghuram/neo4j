/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.StubMultiDatabaseManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static com.neo4j.dbms.OperatorState.STARTED;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;

@ExtendWith( LifeExtension.class )
class DbmsReconcilerModuleTest
{

    @Inject
    private LifeSupport lifeSupport;
    private StubMultiDatabaseManager databaseManager;
    private final TestDatabaseIdRepository idRepository = new TestDatabaseIdRepository();
    private final EnterpriseSystemGraphDbmsModel dbmsModel = mock( EnterpriseSystemGraphDbmsModel.class );
    private final JobScheduler jobScheduler = new ThreadPoolJobScheduler();

    @BeforeEach
    void setup()
    {
        lifeSupport.add( jobScheduler );
        databaseManager = lifeSupport.add( new StubMultiDatabaseManager( jobScheduler ) );
    }

    @Test
    void shouldStartInitialDatabases()
    {
        // given
        when( dbmsModel.getDatabaseStates() ).thenReturn(
                singletonMap( idRepository.defaultDatabase().name(), new DatabaseState( idRepository.defaultDatabase(), STARTED ) ) );
        var reconcilerModule = new StandaloneDbmsReconcilerModule( databaseManager.globalModule(), databaseManager,
                mock( ReconciledTransactionTracker.class ), dbmsModel );

        // when
        reconcilerModule.start();

        // then
        var system = databaseManager.getDatabaseContext( SYSTEM_DATABASE_ID );
        var neo4j = databaseManager.getDatabaseContext( idRepository.defaultDatabase() );
        assertTrue( system.isPresent(), "System db should have been created" );
        assertTrue( neo4j.isPresent(), "Default db should have been created" );
        verify( system.get().database() ).start();
        verify( neo4j.get().database() ).start();
    }

    @Test
    void shouldStopAllDatabases()
    {
        // given
        var fooId = idRepository.getRaw( "foo" );
        var barId = idRepository.getRaw( "bar" );
        var bazId = idRepository.getRaw( "baz" );

        Map<String, DatabaseState> desiredDbStates = Stream.of( fooId, barId, bazId )
                .collect( Collectors.toMap( DatabaseId::name, id -> new DatabaseState( id, STARTED ) ) );

        when( dbmsModel.getDatabaseStates() ).thenReturn( desiredDbStates );
        var reconcilerModule = new StandaloneDbmsReconcilerModule( databaseManager.globalModule(), databaseManager,
                mock( ReconciledTransactionTracker.class ), dbmsModel );
        reconcilerModule.start();

        Function<DatabaseId,Stream<Database>> getDb = ( DatabaseId id ) -> databaseManager.getDatabaseContext( id )
                .map( DatabaseContext::database )
                .stream();

        var databases = Stream.of( fooId, barId, bazId, SYSTEM_DATABASE_ID )
                .flatMap( getDb )
                .collect( Collectors.toList() );

        // when
        reconcilerModule.stop();

        // then
        assertEquals( 4, databases.size(), "4 databases should have been created" );
        for ( Database database : databases )
        {
            verify( database ).stop();
        }
    }
}
