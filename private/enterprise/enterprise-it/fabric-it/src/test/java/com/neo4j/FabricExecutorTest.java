/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.fabric.driver.DriverPool;
import com.neo4j.fabric.driver.FabricDriverTransaction;
import com.neo4j.fabric.driver.PooledDriver;
import com.neo4j.fabric.stream.Records;
import com.neo4j.fabric.stream.StatementResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.internal.SessionConfig;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FabricExecutorTest
{

    private final PortUtils.Ports ports = PortUtils.findFreePorts();

    private final Map<String, String> configProperties = Map.of(
            "fabric.database.name", "mega",
            "fabric.graph.0.uri", "bolt://localhost:1111",
            "fabric.graph.1.uri", "bolt://localhost:2222",
            "fabric.routing.servers", "localhost:" + ports.bolt,
            "dbms.connector.bolt.listen_address", "0.0.0.0:" + ports.bolt,
            "dbms.connector.bolt.enabled", "true"
    );

    private final Config config = Config.newBuilder()
            .setRaw(configProperties)
            .build();

    private TestServer testServer;
    private Driver clientDriver;
    private ArgumentCaptor<org.neo4j.bolt.runtime.AccessMode> accessModeArgument = ArgumentCaptor.forClass( org.neo4j.bolt.runtime.AccessMode.class );
    private StatementResult mockStatementResult;

    PooledDriver createMockDriver()
    {
        PooledDriver mockDriver = mock( PooledDriver.class );

        FabricDriverTransaction tx = mock( FabricDriverTransaction.class );
        mockStatementResult = mock( StatementResult.class );
        when( mockStatementResult.columns() ).thenReturn( Flux.fromIterable( List.of( "a", "b" ) ) );
        when( mockStatementResult.records() ).thenReturn( Flux.empty() );

        when( tx.run( any(), any() ) ).thenReturn( mockStatementResult );
        when( tx.commit() ).thenReturn( Mono.empty() );
        when( tx.rollback() ).thenReturn( Mono.empty() );

        when( mockDriver.beginTransaction(any(), accessModeArgument.capture(), any() )).thenReturn( Mono.just( tx ) );
        return mockDriver;
    }

    DriverPool createMockDriverPool( PooledDriver pooledDriver )
    {
        DriverPool pool = mock( DriverPool.class );
        when( pool.getDriver( any(), any() ) ).thenReturn( pooledDriver );
        return pool;
    }

    @BeforeEach
    void setUp()
    {
        testServer = new TestServer( config );

        PooledDriver mockDriver = createMockDriver();

        testServer.addMocks( createMockDriverPool( mockDriver )  );
        testServer.start();

        clientDriver = GraphDatabase.driver(
                "bolt://localhost:" + ports.bolt,
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                        .withMaxConnectionPoolSize( 3 )
                        .withoutEncryption()
                        .build() );
    }

    @AfterEach
    void tearDown()
    {
        testServer.stop();
        clientDriver.close();
    }

    private Transaction transaction( String database, AccessMode mode )
    {
        return clientDriver.session( SessionConfig.builder().withDatabase( database ).withDefaultAccessMode( mode ).build() ).beginTransaction();
    }

    @Test
    void testReadInReadSession()
    {
        Transaction tx = transaction( "mega", AccessMode.READ );
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();
        tx.success();

        verifySessionConfig( 2, org.neo4j.bolt.runtime.AccessMode.READ );
    }

    @Test
    void testReadInWriteSession()
    {
        Transaction tx = transaction( "mega", AccessMode.WRITE );
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();
        tx.success();

        verifySessionConfig( 2, org.neo4j.bolt.runtime.AccessMode.WRITE );
    }

    @Test
    void testWriteInReadSession()
    {
        Transaction tx = transaction( "mega", AccessMode.READ );
        tx.run( "FROM mega.graph0 CREATE (n:Foo)" ).consume();
        tx.run( "FROM mega.graph0 CREATE (n:Foo)" ).consume();
        tx.success();

        verifySessionConfig( 1, org.neo4j.bolt.runtime.AccessMode.READ );
    }

    @Test
    void testWriteInWriteSession()
    {
        Transaction tx = transaction( "mega", AccessMode.WRITE );
        tx.run( "FROM mega.graph0 CREATE (n:Foo)" ).consume();
        tx.run( "FROM mega.graph0 CREATE (n:Foo)" ).consume();

        verifySessionConfig( 1, org.neo4j.bolt.runtime.AccessMode.WRITE );

        assertThrows( DatabaseException.class,
                () -> tx.run( "FROM mega.graph1 CREATE (n:Foo)" ).consume()
        );
    }

    @Test
    void testMixedInReadSession()
    {
        Transaction tx = transaction( "mega", AccessMode.READ );
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph0 CREATE (n:Foo)" ).consume();
        tx.run( "FROM mega.graph0 CREATE (n:Foo)" ).consume();
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 CREATE (n:Foo)" ).consume();
        tx.run( "FROM mega.graph1 CREATE (n:Foo)" ).consume();

        verifySessionConfig( 2, org.neo4j.bolt.runtime.AccessMode.READ );
    }

    @Test
    void testMixedInWriteSession()
    {
        Transaction tx = transaction( "mega", AccessMode.WRITE );
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph0 CREATE (n:Foo)" ).consume();
        tx.run( "FROM mega.graph0 CREATE (n:Foo)" ).consume();
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph0 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();
        tx.run( "FROM mega.graph1 MATCH (n) RETURN n" ).consume();

        verifySessionConfig( 2, org.neo4j.bolt.runtime.AccessMode.WRITE );

        assertThrows( DatabaseException.class,
                () -> tx.run( "FROM mega.graph1 CREATE (n:Foo)" ).consume()
        );
    }

    @Test
    void testLocalReturnQuery()
    {
        Transaction tx = transaction( "mega", AccessMode.READ );
        List<Record> list = tx.run( String.join( "\n",
                "RETURN 1 AS x"
        ) ).list();
        tx.success();

        assertEquals( list.get( 0 ).get( "x" ).asLong(), 1 );
    }

    @Test
    void testLocalFlatCompositeQuery()
    {
        Transaction tx = transaction( "mega", AccessMode.READ );
        List<Record> list = tx.run( String.join( "\n",
                "UNWIND [1, 2] AS x",
                "CALL { RETURN 'y' AS y }",
                "CALL { UNWIND [8, 9] AS z RETURN z }",
                "RETURN x, y, z ORDER BY x, y, z"
        ) ).list();
        tx.success();

        assertEquals( list.get( 0 ).get( "x" ).asLong(), 1 );
        assertEquals( list.get( 0 ).get( "y" ).asString(), "y" );
        assertEquals( list.get( 0 ).get( "z" ).asLong(), 8 );

        assertEquals( list.get( 1 ).get( "x" ).asLong(), 1 );
        assertEquals( list.get( 1 ).get( "y" ).asString(), "y" );
        assertEquals( list.get( 1 ).get( "z" ).asLong(), 9 );

        assertEquals( list.get( 2 ).get( "x" ).asLong(), 2 );
        assertEquals( list.get( 2 ).get( "y" ).asString(), "y" );
        assertEquals( list.get( 2 ).get( "z" ).asLong(), 8 );

        assertEquals( list.get( 3 ).get( "x" ).asLong(), 2 );
        assertEquals( list.get( 3 ).get( "y" ).asString(), "y" );
        assertEquals( list.get( 3 ).get( "z" ).asLong(), 9 );
    }

    @Test
    void testRemoteFlatCompositeQuery()
    {
        when( mockStatementResult.records() ).thenReturn(
                recs( rec( Values.stringValue( "a" )), rec( Values.stringValue( "b"  ) )) ,
                recs( rec( Values.stringValue( "k" )), rec( Values.stringValue( "l"  ) ))
        );

        Transaction tx = transaction( "mega", AccessMode.READ );
        List<Record> list = tx.run( String.join( "\n",
                "UNWIND [0, 1] AS s",
                "CALL { FROM mega.graph(s) RETURN '' AS y }",
                "RETURN s, y ORDER BY s, y"
        ) ).list();
        tx.success();

        assertEquals( list.get( 0 ).get( "s" ).asLong(), 0 );
        assertEquals( list.get( 0 ).get( "y" ).asString(), "a" );

        assertEquals( list.get( 1 ).get( "s" ).asLong(), 0 );
        assertEquals( list.get( 1 ).get( "y" ).asString(), "b" );

        assertEquals( list.get( 2 ).get( "s" ).asLong(), 1 );
        assertEquals( list.get( 2 ).get( "y" ).asString(), "k" );

        assertEquals( list.get( 3 ).get( "s" ).asLong(), 1 );
        assertEquals( list.get( 3 ).get( "y" ).asString(), "l" );
    }

    private Flux<com.neo4j.fabric.stream.Record> recs( com.neo4j.fabric.stream.Record... records )
    {
        return Flux.just( records );
    }

    private com.neo4j.fabric.stream.Record rec( AnyValue... vals )
    {
        return Records.of(vals);
    }

    private void verifySessionConfig( int times, org.neo4j.bolt.runtime.AccessMode accessMode )
    {
        var allValues = accessModeArgument.getAllValues();

        assertEquals( times, allValues.size() );
        assertTrue( allValues.stream().allMatch( ac -> ac.equals( accessMode ) ) );
    }
}
