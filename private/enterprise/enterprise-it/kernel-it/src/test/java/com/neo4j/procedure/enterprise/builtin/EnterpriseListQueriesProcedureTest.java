/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.concurrent.ThreadingExtension;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.cypher_hints_error;
import static org.neo4j.configuration.GraphDatabaseSettings.track_query_allocation;
import static org.neo4j.configuration.GraphDatabaseSettings.track_query_cpu_time;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.test.rule.concurrent.ThreadingRule.waitingWhileIn;

@EnterpriseDbmsExtension( configurationCallback = "configure" )
@ExtendWith( ThreadingExtension.class )
public class EnterpriseListQueriesProcedureTest
{
    @Inject
    private GraphDatabaseService db;
    @Inject
    private ThreadingRule threads;

    private static final int SECONDS_TIMEOUT = 240;
    private static final Condition<Object> LONG_VALUE = new Condition<>( value -> value instanceof Long, "long value" );

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( cypher_hints_error, true )
               .setConfig( GraphDatabaseSettings.track_query_allocation, true )
               .setConfig( track_query_cpu_time, true );
    }

    @Test
    void shouldProvideElapsedCpuTimePlannerConnectionDetailsPageHitsAndFaults() throws Exception
    {
        // given
        String query = "MATCH (n) SET n.v = n.v + 1";
        db.executeTransactionally( "call dbms.setConfigValue('" + track_query_cpu_time.name() + "', 'true')" );

        try ( Resource<Node> test = test( Transaction::createNode, query ) )
        {
            // when
            Map<String,Object> data = getQueryListing( query );

            // then
            assertThat( data ).containsKey( "elapsedTimeMillis" );
            Object elapsedTime = data.get( "elapsedTimeMillis" );
            assertThat( elapsedTime ).isInstanceOf( Long.class );
            assertThat( data ).containsKey( "cpuTimeMillis" );
            Object cpuTime1 = data.get( "cpuTimeMillis" );
            assertThat( cpuTime1 ).isInstanceOf( Long.class );
            assertThat( data ).containsKey( "resourceInformation" );
            Object ri = data.get( "resourceInformation" );
            assertThat( ri ).isInstanceOf( Map.class );
            @SuppressWarnings( "unchecked" )
            Map<String,Object> resourceInformation = (Map<String,Object>) ri;
            assertEquals( "waiting", data.get( "status" ) );
            assertEquals( "EXCLUSIVE", resourceInformation.get( "lockMode" ) );
            assertEquals( "NODE", resourceInformation.get( "resourceType" ) );
            assertArrayEquals( new long[] {test.resource().getId()}, (long[]) resourceInformation.get( "resourceIds" ) );
            assertThat( data ).containsKey( "waitTimeMillis" );
            Object waitTime1 = data.get( "waitTimeMillis" );
            assertThat( waitTime1 ).isInstanceOf( Long.class );

            // when
            data = getQueryListing( query );

            // then
            Long cpuTime2 = (Long) data.get( "cpuTimeMillis" );
            assertThat( cpuTime2 ).isGreaterThanOrEqualTo( (Long) cpuTime1 );
            Long waitTime2 = (Long) data.get( "waitTimeMillis" );
            assertThat( waitTime2 ).isGreaterThanOrEqualTo( (Long) waitTime1 );

            // ListPlannerAndRuntimeUsed
            // then
            assertThat( data ).containsKey( "planner" );
            assertThat( data ).containsKey( "runtime" );
            assertThat( data.get( "planner" ) ).isInstanceOf( String.class );
            assertThat( data.get( "runtime" ) ).isInstanceOf( String.class );

            // SpecificConnectionDetails

            // then
            assertThat( data ).containsKey( "protocol" );
            assertThat( data ).containsKey( "connectionId" );
            assertThat( data ).containsKey( "clientAddress" );
            assertThat( data ).containsKey( "requestUri" );

            assertThat( data ).hasEntrySatisfying( "pageHits", LONG_VALUE );
            assertThat( data ).hasEntrySatisfying( "pageFaults", LONG_VALUE );
        }
    }

    @Test
    void shouldProvideAllocatedBytes() throws Exception
    {
        // given
        String query = "MATCH (n) WITH n ORDER BY n SET n.v = n.v + 1";
        db.executeTransactionally(  "CALL dbms.setConfigValue('" + track_query_allocation.name() + "', 'true')" );
        final Node node;
        Object allocatedBytes;
        try ( Resource<Node> test = test( Transaction::createNode, query ) )
        {
            node = test.resource();
            // when
            Map<String,Object> data = getQueryListing( query );

            // then
            assertThat( data ).containsKey( "allocatedBytes" );
            allocatedBytes = data.get( "allocatedBytes" );
            assertThat( allocatedBytes ).isInstanceOf( Long.class ).satisfies( value -> assertThat( (Long) value ).isGreaterThan( 0 ) );
        }

        try ( Resource<Node> test = test( tx -> node, query ) )
        {
            // when
            Map<String,Object> data = getQueryListing( query );

            assertThat( data ).containsKey( "allocatedBytes" );
            assertThat( data.get( "allocatedBytes" ) ).isEqualTo( allocatedBytes );
            assertSame( node, test.resource() );
        }
    }
    
    @Test
    void shouldListActiveLocks() throws Exception
    {
        // given
        String query = "MATCH (x:X) SET x.v = 5 WITH count(x) AS num MATCH (y:Y) SET y.c = num";

        // Run the query one time first so that the plan is cached and
        // locks taken during planning is not counted in
        db.executeTransactionally( query );

        Set<Long> locked = new HashSet<>();
        try ( Resource<Node> test = test( tx ->
        {
            for ( int i = 0; i < 5; i++ )
            {
                locked.add( tx.createNode( label( "X" ) ).getId() );
            }
            return tx.createNode( label( "Y" ) );
        }, query ) )
        {
            // when
            try ( Transaction transaction = db.beginTx() )
            {
                try ( Result rows = transaction.execute(
                        "CALL dbms.listQueries() " + "YIELD query AS queryText, queryId, activeLockCount " + "WHERE queryText = $queryText " +
                                "CALL dbms.listActiveLocks(queryId) YIELD mode, resourceType, resourceId " + "RETURN *", singletonMap( "queryText", query ) ); )
                {
                    // then
                    Set<Long> ids = new HashSet<>();
                    Long lockCount = null;
                    long rowCount = 0;
                    while ( rows.hasNext() )
                    {
                        Map<String,Object> row = rows.next();
                        Object resourceType = row.get( "resourceType" );
                        Object activeLockCount = row.get( "activeLockCount" );
                        if ( lockCount == null )
                        {
                            assertThat( activeLockCount ).as( "activeLockCount" ).isInstanceOf( Long.class );
                            lockCount = (Long) activeLockCount;
                        }
                        else
                        {
                            assertEquals( lockCount, activeLockCount, "activeLockCount" );
                        }
                        if ( ResourceTypes.LABEL.name().equals( resourceType ) )
                        {
                            assertEquals( "SHARED", row.get( "mode" ) );
                            assertEquals( 0L, row.get( "resourceId" ) );
                        }
                        else
                        {
                            assertEquals( "NODE", resourceType );
                            assertEquals( "EXCLUSIVE", row.get( "mode" ) );
                            ids.add( (Long) row.get( "resourceId" ) );
                        }
                        rowCount++;
                    }
                    assertEquals( locked, ids );
                    assertNotNull( lockCount, "activeLockCount" );
                    assertEquals( lockCount.intValue(), rowCount ); // note: only true because query is blocked
                }
                transaction.commit();
            }
        }
    }

    @Test
    void shouldOnlyGetActiveLockCountFromCurrentQuery() throws Exception
    {
        // given
        String query1 = "MATCH (x:X) SET x.v = 1";
        String query2 = "MATCH (y:Y) SET y.v = 2 WITH count(y) AS y MATCH (z:Z) SET z.v = y";
        try ( Resource<Node> test = test( tx ->
        {
            for ( int i = 0; i < 5; i++ )
            {
                tx.createNode( label( "X" ) );
            }
            tx.createNode( label( "Y" ) );
            return tx.createNode( label( "Z" ) );
        }, query1, query2 ) )
        {
            // when
            try ( Transaction transaction = db.beginTx() )
            {
                try ( Result rows = transaction.execute(
                        "CALL dbms.listQueries() " + "YIELD query AS queryText, queryId, activeLockCount " + "WHERE queryText = $queryText " +
                                "CALL dbms.listActiveLocks(queryId) YIELD resourceId " +
                                "WITH queryText, queryId, activeLockCount, count(resourceId) AS allLocks " + "RETURN *",
                        singletonMap( "queryText", query2 ) ); )
                {
                    assertTrue( rows.hasNext(), "should have at least one row" );
                    Map<String,Object> row = rows.next();
                    Object activeLockCount = row.get( "activeLockCount" );
                    Object allLocks = row.get( "allLocks" );
                    assertFalse( rows.hasNext(), "should have at most one row" );
                    assertThat( activeLockCount ).as( "activeLockCount" ).isInstanceOf( Long.class );
                    assertThat( allLocks ).as( "allLocks" ).isInstanceOf( Long.class );
                    assertThat( (Long) activeLockCount ).isLessThan( (Long) allLocks );
                }
                transaction.commit();
            }
        }
    }

    @Test
    void shouldDisableCpuTimeTracking() throws Exception
    {
        // given
        String query = "MATCH (n) SET n.v = n.v + 1";
        db.executeTransactionally( "CALL dbms.setConfigValue('" + track_query_cpu_time.name() + "', 'false')" );
        Map<String,Object> data;

        // when
        try ( Resource<Node> test = test( Transaction::createNode, query ) )
        {
            data = getQueryListing( query );
        }

        // then
        assertThat( data ).hasEntrySatisfying( "cpuTimeMillis",  value -> assertThat( value ).isNull() );
    }

    @Test
    void cpuTimeTrackingShouldBeADynamicSetting() throws Exception
    {
        // given
        String query = "MATCH (n) SET n.v = n.v + 1";
        Map<String,Object> data;

        // when
        try ( Resource<Node> test = test( Transaction::createNode, query ) )
        {
            data = getQueryListing( query );
        }
        // then
        assertThat( data ).hasEntrySatisfying( "cpuTimeMillis", value -> assertThat( value ).isNotNull() );

        // when
        db.executeTransactionally( "call dbms.setConfigValue('" + track_query_cpu_time.name() + "', 'false')" );
        try ( Resource<Node> test = test( Transaction::createNode, query ) )
        {
            data = getQueryListing( query );
        }
        // then
        assertThat( data ).hasEntrySatisfying( "cpuTimeMillis", value -> assertThat( value ).isNull() );

        // when
        db.executeTransactionally( "call dbms.setConfigValue('" + track_query_cpu_time.name() + "', 'true')" );
        try ( Resource<Node> test = test( Transaction::createNode, query ) )
        {
            data = getQueryListing( query );
        }
        // then
        assertThat( data ).hasEntrySatisfying( "cpuTimeMillis", value -> assertThat( value ).isNotNull() );
    }

    @Test
    void shouldDisableHeapAllocationTracking() throws Exception
    {
        // given
        String query = "MATCH (n) WITH n ORDER BY n SET n.v = n.v + 1";
        db.executeTransactionally(  "CALL dbms.setConfigValue('" + track_query_allocation.name() + "', 'false')" );
        Map<String,Object> data;

        // when
        try ( Resource<Node> test = test( Transaction::createNode, query ) )
        {
            data = getQueryListing( query );
        }

        // then
        assertThat( data ).hasEntrySatisfying( "allocatedBytes", value -> assertThat( value ).isNull() );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    void heapAllocationTrackingShouldBeADynamicSetting() throws Exception
    {
        // given
        String query = "MATCH (n) WITH n ORDER BY n SET n.v = n.v + 1";
        Map<String,Object> data;

        // when
        try ( Resource<Node> test = test( Transaction::createNode, query ) )
        {
            data = getQueryListing( query );
        }
        // then
        assertThat( data ).hasEntrySatisfying( "allocatedBytes",
                value -> assertThat( value ).isInstanceOf( Long.class ).satisfies( v -> assertThat( (Long) v ).isGreaterThan( 0 ) ) );

        // when
        db.executeTransactionally( "call dbms.setConfigValue('" + track_query_allocation.name() + "', 'false')" );
        try ( Resource<Node> test = test( Transaction::createNode, query ) )
        {
            data = getQueryListing( query );
        }
        // then
        assertThat( data ).hasEntrySatisfying( "allocatedBytes", value -> assertThat( value ).isNull() );

        // when
        db.executeTransactionally( "call dbms.setConfigValue('" + track_query_allocation.name() + "', 'true')" );
        try ( Resource<Node> test = test( Transaction::createNode, query ) )
        {
            data = getQueryListing( query );
        }
        // then
        assertThat( data ).hasEntrySatisfying( "allocatedBytes",
                value -> assertThat( value ).isInstanceOf( Long.class ).satisfies( v -> assertThat( (Long) v ).isGreaterThan( 0 ) ) );
    }

    private Map<String,Object> getQueryListing( String query )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            try ( Result rows = transaction.execute( "CALL dbms.listQueries" ) )
            {
                while ( rows.hasNext() )
                {
                    Map<String,Object> row = rows.next();
                    if ( query.equals( row.get( "query" ) ) )
                    {
                        return row;
                    }
                }
            }
            transaction.commit();
        }
        throw new AssertionError( "query not active: " + query );
    }

    private static class Resource<T> implements AutoCloseable
    {
        private final CountDownLatch latch;
        private final CountDownLatch finishLatch;
        private final T resource;

        private Resource( CountDownLatch latch, CountDownLatch finishLatch, T resource )
        {
            this.latch = latch;
            this.finishLatch = finishLatch;
            this.resource = resource;
        }

        @Override
        public void close() throws InterruptedException
        {
            latch.countDown();
            finishLatch.await();
        }

        public T resource()
        {
            return resource;
        }
    }

    private <T extends Entity> Resource<T> test( Function<Transaction, T> setup, String... queries )
            throws InterruptedException, ExecutionException
    {
        CountDownLatch resourceLocked = new CountDownLatch( 1 );
        CountDownLatch listQueriesLatch = new CountDownLatch( 1 );
        CountDownLatch finishQueriesLatch = new CountDownLatch( 1 );
        T resource;
        try ( Transaction tx = db.beginTx() )
        {
            resource = setup.apply(tx);
            tx.commit();
        }
        threads.execute( parameter ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.acquireWriteLock( resource );
                resourceLocked.countDown();
                listQueriesLatch.await();
            }
            return null;
        }, null );
        resourceLocked.await();

        threads.executeAndAwait( parameter ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                for ( String query : queries )
                {
                    tx.execute( query ).close();
                }
                tx.commit();
            }
            catch ( Throwable t )
            {
                throw new RuntimeException( t );
            }
            finally
            {
                finishQueriesLatch.countDown();
            }
            return null;
        }, null, waitingWhileIn( Transaction.class, "execute" ), SECONDS_TIMEOUT, SECONDS );

        return new Resource<>( listQueriesLatch, finishQueriesLatch, resource );
    }
}
