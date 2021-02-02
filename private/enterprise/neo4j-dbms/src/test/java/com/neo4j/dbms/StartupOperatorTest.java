/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

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
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static java.util.function.Function.identity;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

class StartupOperatorTest
{
    private EnterpriseSystemGraphDbmsModel dbmsModel = mock( EnterpriseSystemGraphDbmsModel.class );
    private int startBatchSize = 4;
    private Config config = Config.defaults( GraphDatabaseSettings.reconciler_maximum_parallelism, startBatchSize );
    private StartupOperator operator = new StartupOperator( dbmsModel, config, NullLogProvider.nullLogProvider() );
    private DbmsReconciler dbmsReconciler = mock( DbmsReconciler.class );
    private TestOperatorConnector connector = new TestOperatorConnector( dbmsReconciler );
    private List<NamedDatabaseId> databases =
            Stream.concat( Stream.of( NAMED_SYSTEM_DATABASE_ID ), Stream.generate( TestDatabaseIdRepository::randomNamedDatabaseId ) )
                    .limit( 10 ).collect( Collectors.toList() );

    @BeforeEach
    void setup()
    {
        when( dbmsReconciler.reconcile( anyList(), any() ) ).thenReturn( ReconcilerResult.EMPTY );
        operator.connect( connector );
        when( dbmsModel.getDatabaseStates() ).thenReturn( databases.stream().collect( Collectors.toMap(
                NamedDatabaseId::name, db -> new EnterpriseDatabaseState( db, STARTED ) ) ) );
    }

    @Test
    void shouldStartSystemDatabase()
    {
        operator.startSystem();
        assertTrue( operator.desired().isEmpty() );

        var triggerCalls = connector.triggerCalls();

        var firstDesired = triggerCalls.get( 0 ).first();
        var expectedFirst = Map.of( SYSTEM_DATABASE_NAME, new EnterpriseDatabaseState( NAMED_SYSTEM_DATABASE_ID, STARTED ) );
        assertEquals( expectedFirst, firstDesired );

        triggerCalls.stream().skip( 1 ).forEach( call -> assertFalse( call.first().containsKey( SYSTEM_DATABASE_NAME ) ) );
    }

    @Test
    void shouldStartAllDatabases()
    {
        operator.startAllNonSystem();
        assertTrue( operator.desired().isEmpty() );

        var triggerCalls = connector.triggerCalls();

        var allDesired = triggerCalls.stream().flatMap( batch -> batch.first().values().stream() ).collect( Collectors.toSet() );

        var expected = databases.stream().filter( id -> !id.name().equals( SYSTEM_DATABASE_NAME ) )
                .map( id -> new EnterpriseDatabaseState( id, STARTED ) ).collect( Collectors.toSet() );
        assertEquals( expected, allDesired );
    }

    @Test
    void shouldStartAllDatabasesInBatches()
    {
        assertTrue( operator.desired().isEmpty() );
        operator.startSystem();
        operator.startAllNonSystem();
        assertTrue( operator.desired().isEmpty() );

        var triggerCalls = connector.triggerCalls();

        var expectedTriggerCalls = 1 + (int) Math.ceil( (double) (databases.size() - 1) / startBatchSize );
        assertEquals( expectedTriggerCalls, triggerCalls.size() );

        var desiredPerTrigger = triggerCalls.stream().map( Pair::first ).collect( Collectors.toList() );

        var batches = new ArrayList<Map<String,EnterpriseDatabaseState>>();
        var orderedDatabaseNames = new ArrayList<String>();

        for ( var desired : desiredPerTrigger )
        {
            var desiredCopy = new HashMap<>( desired );
            batches.add( desiredCopy );
            orderedDatabaseNames.addAll( desiredCopy.keySet().stream().sorted().collect( Collectors.toList() ) );
        }

        var allExpectedDatabaseNames = Stream.concat( Stream.of( SYSTEM_DATABASE_NAME ),
                databases.stream().skip( 1 ).map( NamedDatabaseId::name ).sorted() ).collect( Collectors.toList() );

        var totalSize = batches.stream().mapToInt( Map::size ).sum();
        var largestBatch = batches.stream().mapToInt( Map::size ).max().getAsInt();
        var databaseNameCounts = batches.stream().flatMap( map -> map.keySet().stream() ).collect( Collectors.groupingBy( identity(), Collectors.counting() ) );
        var duplicates = databaseNameCounts.values().stream().anyMatch( count -> count > 1 );

        assertFalse( duplicates, "No database appears in more than one batch" );
        assertThat( "DatabaseNames should be in alphabetical order", orderedDatabaseNames, equalTo( allExpectedDatabaseNames ) );
        assertThat( "Largest batch should be less than or equal to batch size", largestBatch, lessThanOrEqualTo( startBatchSize ) );
        assertEquals( databases.size(), totalSize, "Total size of all batches should be same as number of databases" );
    }
}
