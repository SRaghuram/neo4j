/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.fabric.driver.DriverPool;
import com.neo4j.fabric.driver.PooledDriver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.SessionConfig;
import org.neo4j.driver.internal.shaded.reactor.core.publisher.Flux;
import org.neo4j.driver.internal.shaded.reactor.core.publisher.Mono;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxStatementResult;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

class ParameterConversionTest
{
    private static Driver clientDriver;
    private static TestServer testServer;
    private static DriverPool driverPool = mock( DriverPool.class );
    private static Driver shardDriver = mock( Driver.class );

    private final ArgumentCaptor<Map<String,Object>> parameterCaptor = ArgumentCaptor.forClass( Map.class );

    @BeforeAll
    static void beforeAll()
    {
        var ports = PortUtils.findFreePorts();
        var configProperties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", "bolt://somewhere:1234",
                "fabric.routing.servers", "localhost:" + ports.bolt,
                "dbms.connector.bolt.listen_address", "0.0.0.0:" + ports.bolt,
                "dbms.connector.bolt.enabled", "true");
        var config = Config.newBuilder().set( configProperties ).build();

        testServer = new TestServer( config );
        testServer.addMocks( driverPool );
        testServer.start();

        clientDriver = GraphDatabase.driver( "bolt://localhost:" + ports.bolt, AuthTokens.none(), org.neo4j.driver.Config.builder()
                .withMaxConnectionPoolSize( 3 )
                .build() );

        PooledDriver pooledDriver = mock( PooledDriver.class );
        when( pooledDriver.getDriver() ).thenReturn( shardDriver );
        when( driverPool.getDriver( any(), any() ) ).thenReturn( pooledDriver );
    }

    @AfterAll
    static void afterAll()
    {
        testServer.stop();
        clientDriver.close();
    }

    @BeforeEach
    void beforeEach()
    {
        reset( shardDriver );
        RxSession mockDriverSession = mock( RxSession.class );
        RxTransaction tx = mock( RxTransaction.class );
        RxStatementResult mockDriverResult = mock( RxStatementResult.class);
        when(mockDriverResult.keys()).thenReturn( Flux.fromIterable( Arrays.asList("a", "b" )));
        when( mockDriverResult.records() ).thenReturn( Mono.empty() );

        when( tx.run( any(), parameterCaptor.capture() ) ).thenReturn( mockDriverResult );
        when( tx.commit() ).thenReturn( Mono.empty() );
        when( tx.rollback() ).thenReturn( Mono.empty() );

        when( mockDriverSession.beginTransaction() ).thenReturn( Mono.just( tx ) );
        when( mockDriverSession.close() ).thenReturn( Mono.empty() );
        when( shardDriver.rxSession( any() ) ).thenReturn( mockDriverSession );
    }

    @Test
    void testNull()
    {
        Map<String,Object> params = new HashMap<>();
        params.put( "p", null );

        execute( "Return $p", params );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNull( paramValue );
    }

    @Test
    void testInteger()
    {
        execute( "Return $p", Map.of( "p", 1234 ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( 1234L, paramValue );
    }

    @Test
    void testDouble()
    {
        execute( "Return $p", Map.of( "p", 1234.99 ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( 1234.99, paramValue );
    }

    @Test
    void testString()
    {
        execute( "Return $p", Map.of( "p", "some value" ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( "some value", paramValue );
    }

    @Test
    void testBytes()
    {
        byte[] bytes = "Hello".getBytes();
        execute( "Return $p", Map.of( "p", bytes ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertArrayEquals( bytes, (byte[]) paramValue );
    }

    @Test
    void testBoolean()
    {
        execute( "Return $p", Map.of( "p", true ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( true, paramValue );
    }

    @Test
    void testUniformList()
    {
        List<String> list = List.of( "Hello", "Hi" );
        execute( "Return $p", Map.of( "p", list ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( list, paramValue );
    }

    @Test
    void testHeterogeneousList()
    {
        List<Object> list = List.of( "Hello", 1234L );
        execute( "Return $p", Map.of( "p", list ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( list, paramValue );
    }

    @Test
    void testSimpleMap()
    {
        Map<String,Object> map = Map.of( "ke1", "Hello", "key2", 1234L );
        execute( "Return $p", Map.of( "p", map ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( map, paramValue );
    }

    @Test
    void testNestedMap()
    {
        Map<String,Object> map = Map.of( "key", "Hello", "listKey", List.of( Map.of( "inner", "Map1" ), Map.of( "inner", "Map2" ) ) );
        execute( "Return $p", Map.of( "p", map ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( map, paramValue );
    }

    @Test
    void testDate()
    {
        LocalDate date = LocalDate.parse( "2019-04-11" );
        execute( "Return $p", Map.of( "p", date ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( date, paramValue );
    }

    @Test
    void testLocalTime()
    {
        LocalTime localTime = LocalTime.parse( "11:55" );
        execute( "Return $p", Map.of( "p", localTime ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( localTime, paramValue );
    }

    @Test
    void testLocalDateTime()
    {
        LocalDateTime localDateTime = LocalDateTime.parse( "2019-04-11T11:55" );
        execute( "Return $p", Map.of( "p", localDateTime ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( localDateTime, paramValue );
    }

    @Test
    void testTime()
    {
        ZonedDateTime time = ZonedDateTime.parse( "2015-06-24T12:50:35.556+01:00" );
        execute( "Return $p", Map.of( "p", time ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( time, paramValue );
    }

    @Test
    void testDateTime()
    {
        ZonedDateTime dateTime = ZonedDateTime.parse( "2015-06-24T12:50:35.556+01:00" );
        execute( "Return $p", Map.of( "p", dateTime ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( dateTime, paramValue );
    }

    @Test
    void testDuration()
    {
        Value duration = Values.isoDuration( 3, 14, 12, 111 );
        execute( "Return $p", Map.of( "p", duration ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( duration, paramValue );
    }

    @Test
    void testCartesian2DPoint()
    {
        Value point = Values.point( CoordinateReferenceSystem.Cartesian.getCode(), 1.0, 2.0 );
        execute( "Return $p", Map.of( "p", point ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( point, paramValue );
    }

    @Test
    void testCartesian3DPoint()
    {
        Value point = Values.point( CoordinateReferenceSystem.Cartesian_3D.getCode(), 1.0, 2.0, 3.0 );
        execute( "Return $p", Map.of( "p", point ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( point, paramValue );
    }

    @Test
    void testWsg2DPoint()
    {
        Value point = Values.point( CoordinateReferenceSystem.WGS84.getCode(), 1.0, 2.0 );
        execute( "Return $p", Map.of( "p", point ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( point, paramValue );
    }

    @Test
    void testWsg3DPoint()
    {
        Value point = Values.point( CoordinateReferenceSystem.WGS84_3D.getCode(), 1.0, 2.0, 3.0 );
        execute( "Return $p", Map.of( "p", point ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( point, paramValue );
    }

    private void execute( String query, Map<String,Object> params )
    {
        try ( Session session = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build()) )
        {
            String shardQuery = "FROM mega.graph0 " + query;
            session.run( shardQuery, params );
        }
    }
}
