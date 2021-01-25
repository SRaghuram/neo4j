/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.StubMultiDatabaseManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static java.util.function.Function.identity;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

class ShutdownOperatorTest
{
    private DatabaseManager<?> databaseManager = new StubMultiDatabaseManager();
    private int stopBatchSize = 4;
    private Config config = Config.defaults( GraphDatabaseSettings.reconciler_maximum_parallelism, stopBatchSize );
    private ShutdownOperator operator = new ShutdownOperator( databaseManager, config );
    private DbmsReconciler dbmsReconciler = mock( DbmsReconciler.class );
    private TestOperatorConnector connector = new TestOperatorConnector( dbmsReconciler );
    private List<NamedDatabaseId> databases =
            Stream.concat( Stream.of( NAMED_SYSTEM_DATABASE_ID ), Stream.generate( TestDatabaseIdRepository::randomNamedDatabaseId ) )
                    .limit( 10 ).collect( Collectors.toList());

    @BeforeEach
    void setup()
    {
        when( dbmsReconciler.reconcile( anyList(), any() ) ).thenReturn( ReconcilerResult.EMPTY );
        operator.connect( connector );
        databases.forEach( databaseManager::createDatabase );
    }

    @Test
    void shouldStopSystemDatabaseLast()
    {
        operator.stopAll();
        var triggerCalls = connector.triggerCalls();

        var lastCall = triggerCalls.size() - 1;
        var penultimateDesired = triggerCalls.get( lastCall - 1 ).first();

        assertThat( "Databases added to operators desired state set should not include system",
                SYSTEM_DATABASE_NAME, not( in( penultimateDesired.keySet() ) ) );

        var lastDesired = triggerCalls.get( lastCall ).first();
        var expectedLastDesired = new HashMap<>( penultimateDesired );
        expectedLastDesired.put( SYSTEM_DATABASE_NAME, new EnterpriseDatabaseState( NAMED_SYSTEM_DATABASE_ID, STOPPED ) );

        assertEquals( expectedLastDesired, lastDesired );
    }

    @Test
    void shouldStopAllDatabases()
    {
        operator.stopAll();
        var triggerCalls = connector.triggerCalls();
        var finalTrigger = triggerCalls.get( triggerCalls.size() - 1 );
        var expected = databases.stream().collect( Collectors.toMap( NamedDatabaseId::name, id -> new EnterpriseDatabaseState( id, STOPPED ) ) );
        assertEquals( expected, finalTrigger.first() );
    }

    @Test
    void shouldStopAllDatabasesInBatches()
    {
        operator.stopAll();
        var triggerCalls = connector.triggerCalls();

        var expectedTriggerCalls = (int) Math.ceil( (double) (databases.size() - 1) / stopBatchSize ) + 1;
        assertEquals( expectedTriggerCalls, triggerCalls.size() );

        var previous = Map.<String,EnterpriseDatabaseState>of();
        var desiredPerTrigger = triggerCalls.stream().map( Pair::first ).collect( Collectors.toList() );

        var batches = new ArrayList<Map<String,EnterpriseDatabaseState>>();

        for ( var desired : desiredPerTrigger )
        {
            var desiredCopy = new HashMap<>( desired );
            desiredCopy.keySet().removeAll( previous.keySet() );
            batches.add( desiredCopy );
            previous = desired;
        }

        var totalSize = batches.stream().mapToInt( Map::size ).sum();
        var largestBatch = batches.stream().mapToInt( Map::size ).max().getAsInt();
        var databaseNames = batches.stream().flatMap( map -> map.keySet().stream() ).collect( Collectors.toSet() );
        var databaseNameCounts = batches.stream().flatMap( map -> map.keySet().stream() ).collect( Collectors.groupingBy( identity(), Collectors.counting() ) );
        var duplicates = databaseNameCounts.values().stream().anyMatch( count -> count > 1 );

        var allExpectedDatabaseNames = databases.stream().map( NamedDatabaseId::name ).collect( Collectors.toSet() );

        assertFalse( duplicates, "No database appears in more than one batch" );
        assertThat( "All databaseNames should be find in batches",  databaseNames, equalTo( allExpectedDatabaseNames ) );
        assertThat( "Largest batch should be less than or equal to batch size", largestBatch, lessThanOrEqualTo( stopBatchSize ) );
        assertEquals( databases.size(), totalSize, "Total size of all batches should be same as number of databases" );
    }
}
