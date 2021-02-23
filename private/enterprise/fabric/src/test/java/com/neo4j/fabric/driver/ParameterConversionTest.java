/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.fabric.executor.ExecutionOptions;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.transaction.FabricTransactionInfo;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.virtual.MapValue;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ParameterConversionTest
{
    private final  Driver databaseDriver = mock( Driver.class );
    private PooledDriver pooledDriver;

    private final ArgumentCaptor<Map<String,Object>> parameterCaptor = ArgumentCaptor.forClass( Map.class );

    @BeforeEach
    void beforeEach()
    {
        pooledDriver = new RxPooledDriver( databaseDriver, d ->
        {
        } );

        RxSession mockDriverSession = mock( RxSession.class );
        RxTransaction tx = mock( RxTransaction.class );
        RxResult mockDriverResult = mock( RxResult.class);
        when(mockDriverResult.keys()).thenReturn( Mono.just( Arrays.asList("a", "b" )));
        when( mockDriverResult.records() ).thenReturn( Mono.empty() );
        when( mockDriverResult.consume() ).thenReturn( Mono.empty() );

        when( tx.run( any(), parameterCaptor.capture() ) ).thenReturn( mockDriverResult );
        when( tx.commit() ).thenReturn( Mono.empty() );
        when( tx.rollback() ).thenReturn( Mono.empty() );

        when( mockDriverSession.beginTransaction( any() ) ).thenReturn( Mono.just( tx ) );
        when( mockDriverSession.close() ).thenReturn( Mono.empty() );
        when( databaseDriver.rxSession( any() ) ).thenReturn( mockDriverSession );
    }

    @Test
    void testNull()
    {
        Map<String,Object> params = new HashMap<>();
        params.put( "p", null );

        execute( params );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNull( paramValue );
    }

    @Test
    void testInteger()
    {
        execute( Map.of( "p", 1234 ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( 1234L, paramValue );
    }

    @Test
    void testDouble()
    {
        execute( Map.of( "p", 1234.99 ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( 1234.99, paramValue );
    }

    @Test
    void testString()
    {
        execute( Map.of( "p", "some value" ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( "some value", paramValue );
    }

    @Test
    void testBytes()
    {
        byte[] bytes = "Hello".getBytes();
        execute( Map.of( "p", bytes ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertArrayEquals( bytes, (byte[]) paramValue );
    }

    @Test
    void testBoolean()
    {
        execute( Map.of( "p", true ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertTrue( (Boolean) paramValue );
    }

    @Test
    void testUniformList()
    {
        List<String> list = List.of( "Hello", "Hi" );
        execute( Map.of( "p", list ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( list, paramValue );
    }

    @Test
    void testHeterogeneousList()
    {
        List<Object> list = List.of( "Hello", 1234L );
        execute( Map.of( "p", list ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( list, paramValue );
    }

    @Test
    void testSimpleMap()
    {
        Map<String,Object> map = Map.of( "ke1", "Hello", "key2", 1234L );
        execute( Map.of( "p", map ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( map, paramValue );
    }

    @Test
    void testNestedMap()
    {
        Map<String,Object> map = Map.of( "key", "Hello", "listKey", List.of( Map.of( "inner", "Map1" ), Map.of( "inner", "Map2" ) ) );
        execute(  Map.of( "p", map ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( map, paramValue );
    }

    @Test
    void testDate()
    {
        LocalDate date = LocalDate.parse( "2019-04-11" );
        execute( Map.of( "p", date ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( date, paramValue );
    }

    @Test
    void testLocalTime()
    {
        LocalTime localTime = LocalTime.parse( "11:55" );
        execute( Map.of( "p", localTime ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( localTime, paramValue );
    }

    @Test
    void testLocalDateTime()
    {
        LocalDateTime localDateTime = LocalDateTime.parse( "2019-04-11T11:55" );
        execute( Map.of( "p", localDateTime ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( localDateTime, paramValue );
    }

    @Test
    void testTime()
    {
        ZonedDateTime time = ZonedDateTime.parse( "2015-06-24T12:50:35.556+01:00" );
        execute( Map.of( "p", time ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( time, paramValue );
    }

    @Test
    void testDateTime()
    {
        ZonedDateTime dateTime = ZonedDateTime.parse( "2015-06-24T12:50:35.556+01:00" );
        execute( Map.of( "p", dateTime ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( dateTime, paramValue );
    }

    @Test
    void testDuration()
    {
        Value duration = Values.isoDuration( 3, 14, 12, 111 );
        execute( Map.of( "p", duration ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( duration, paramValue );
    }

    @Test
    void testCartesian2DPoint()
    {
        Value point = Values.point( CoordinateReferenceSystem.Cartesian.getCode(), 1.0, 2.0 );
        execute( Map.of( "p", point ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( point, paramValue );
    }

    @Test
    void testCartesian3DPoint()
    {
        Value point = Values.point( CoordinateReferenceSystem.Cartesian_3D.getCode(), 1.0, 2.0, 3.0 );
        execute( Map.of( "p", point ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( point, paramValue );
    }

    @Test
    void testWsg2DPoint()
    {
        Value point = Values.point( CoordinateReferenceSystem.WGS84.getCode(), 1.0, 2.0 );
        execute( Map.of( "p", point ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( point, paramValue );
    }

    @Test
    void testWsg3DPoint()
    {
        Value point = Values.point( CoordinateReferenceSystem.WGS84_3D.getCode(), 1.0, 2.0, 3.0 );
        execute( Map.of( "p", point ) );
        Map<String,Object> value = parameterCaptor.getValue();
        Object paramValue = value.get( "p" );
        assertNotNull( paramValue );
        assertEquals( point, paramValue );
    }

    private void execute( Map<String,Object> params )
    {
        var location = mock( Location.Remote.class);
        when( location.getDatabaseName() ).thenReturn( "mega" );
        var transactionIfo = mock( FabricTransactionInfo.class );
        when( transactionIfo.getTxTimeout() ).thenReturn( Duration.ZERO );

        var tx = pooledDriver.beginTransaction( location, new ExecutionOptions(), null, transactionIfo, List.of() ).block();
        var converter = new RecordConverter();
        var driverValue = Values.value( params );
        var serverValue = (MapValue) converter.convertValue( driverValue );

        tx.run( "", serverValue );
    }
}
