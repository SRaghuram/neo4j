/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteArray;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.values.storable.Values.NO_VALUE;

class RecordConverterTest
{

    private static Neo4j testServer;
    private static Driver driver;

    private final RecordConverter converter = new RecordConverter();

    @BeforeAll
    static void setUp()
    {
        testServer = Neo4jBuilders.newInProcessBuilder().withFixture( "CREATE (:Person {name: 'Anna'})" ).build();
        String serverUri = testServer.boltURI().toString();
        driver = GraphDatabase.driver( serverUri, AuthTokens.none(), Config.builder().withoutEncryption().build() );
    }

    @AfterAll
    static void tearDown()
    {
        testServer.close();
        driver.close();
    }

    @Test
    void testNull()
    {
        AnyValue value = executeAndConvert( "RETURN null" );
        assertEquals( NO_VALUE, value );
    }

    @Test
    void testInteger()
    {
        AnyValue value = executeAndConvert( "RETURN 1234" );
        assertTrue( value instanceof LongValue );
        assertEquals( 1234, ((LongValue) value).longValue() );
    }

    @Test
    void testDouble()
    {
        AnyValue value = executeAndConvert( "RETURN 1234.99" );
        assertTrue( value instanceof DoubleValue );
        assertEquals( 1234.99, ((DoubleValue) value).doubleValue() );
    }

    @Test
    void testString()
    {
        AnyValue value = executeAndConvert( "RETURN 'Hello'" );
        assertTrue( value instanceof StringValue );
        assertEquals( "Hello", ((StringValue) value).stringValue() );
    }

    @Test
    void testBytes()
    {
        byte[] bytes = "Hello".getBytes();
        AnyValue value = executeAndConvert( "RETURN $p", Map.of( "p", bytes ) );
        assertTrue( value instanceof ByteArray );
        assertArrayEquals( bytes, ((ByteArray) value).asObjectCopy() );
    }

    @Test
    void testBoolean()
    {
        AnyValue value = executeAndConvert( "RETURN true" );
        assertTrue( value instanceof BooleanValue );
        assertTrue( ((BooleanValue) value).booleanValue() );
    }

    @Test
    void testUniformList()
    {
        AnyValue value = executeAndConvert( "RETURN ['Hello', 'Hi']" );
        assertTrue( value instanceof ListValue );
        ListValue listValue = (ListValue) value;
        assertEquals( 2, listValue.length() );
        assertEquals( "Hello", ((StringValue) listValue.value( 0 )).stringValue() );
        assertEquals( "Hi", ((StringValue) listValue.value( 1 )).stringValue() );
    }

    @Test
    void testHeterogeneousList()
    {
        AnyValue value = executeAndConvert( "RETURN ['Hello', 1234]" );
        assertTrue( value instanceof ListValue );
        ListValue listValue = (ListValue) value;
        assertEquals( 2, listValue.length() );
        assertEquals( "Hello", ((StringValue) listValue.value( 0 )).stringValue() );
        assertEquals( 1234, ((LongValue) listValue.value( 1 )).longValue() );
    }

    @Test
    void testSimpleMap()
    {
        AnyValue value = executeAndConvert( "RETURN { key1 : 'Hello', key2 : 1234 }" );
        assertTrue( value instanceof MapValue );
        MapValue mapValue = (MapValue) value;

        assertEquals( 2, mapValue.size() );
        assertEquals( "Hello", ((StringValue) mapValue.get( "key1" )).stringValue() );
        assertEquals( 1234, ((LongValue) mapValue.get( "key2" )).longValue() );
    }

    @Test
    void testNestedMap()
    {
        AnyValue value = executeAndConvert( "RETURN { key: 'Hello', listKey: [{ inner: 'Map1' }, { inner: 'Map2' }]}" );
        assertTrue( value instanceof MapValue );
        MapValue mapValue = (MapValue) value;
        assertEquals( "Hello", ((StringValue) mapValue.get( "key" )).stringValue() );

        ListValue listValue = (ListValue) mapValue.get( "listKey" );
        MapValue innerMap1 = (MapValue) listValue.value( 0 );
        assertEquals( "Map1", ((StringValue) innerMap1.get( "inner" )).stringValue() );

        MapValue innerMap2 = (MapValue) listValue.value( 1 );
        assertEquals( "Map2", ((StringValue) innerMap2.get( "inner" )).stringValue() );
    }

    @Test
    void testNode()
    {
        AnyValue value = executeAndConvert( "CREATE (n:LABEL_1:LABEL_2 {key1: 'Hello', arrayKey: ['a', 'b' ]}) RETURN n" );
        assertTrue( value instanceof NodeValue );
        NodeValue nodeValue = (NodeValue) value;
        assertEquals( "LABEL_1", nodeValue.labels().stringValue( 0 ) );
        assertEquals( "LABEL_2", nodeValue.labels().stringValue( 1 ) );

        MapValue properties = nodeValue.properties();
        assertEquals( "Hello", ((StringValue) properties.get( "key1" )).stringValue() );

        ListValue list = (ListValue) properties.get( "arrayKey" );
        assertEquals( "a", ((StringValue) list.value( 0 )).stringValue() );
        assertEquals( "b", ((StringValue) list.value( 1 )).stringValue() );
    }

    @Test
    void testNodeWithSourceTag()
    {
        Value n = execute( "CREATE (n:LABEL_1:LABEL_2 {key1: 'Hello', arrayKey: ['a', 'b' ]}) RETURN n" );
        RecordConverter c1 = new RecordConverter( 1 );
        RecordConverter c2 = new RecordConverter( 2 );
        NodeValue n1a = (NodeValue) c1.convertValue( n );
        NodeValue n1b = (NodeValue) c1.convertValue( n );
        NodeValue n2 = (NodeValue) c2.convertValue( n );
        assertEquals( n1a, n1b );
        assertNotEquals( n1a, n2 );
        assertNotEquals( n1b, n2 );
    }

    @Test
    void testNodeWithLargeSourceTag()
    {
        Value n = new org.neo4j.driver.internal.value.NodeValue( new InternalNode( 1L ) );
        RecordConverter c = new RecordConverter( 0x3FFF );
        NodeValue nv = (NodeValue) c.convertValue( n );
        assertEquals( 0xFFFC000000000001L, nv.id() );
    }

    @Test
    void testNodeWithSourceTagAndLargeId()
    {
        Value n = new org.neo4j.driver.internal.value.NodeValue( new InternalNode( 1L << 49 ) );
        RecordConverter c = new RecordConverter( 1 );
        NodeValue nv = (NodeValue) c.convertValue( n );
        assertEquals( 0x0006000000000000L, nv.id() );
    }

    @Test
    void testRelationship()
    {
        AnyValue value = executeAndConvert( "CREATE (n1) - [r:TYPE_1 {key1: 'Hello', arrayKey: ['a', 'b' ]}] -> (n2) RETURN r" );
        assertTrue( value instanceof RelationshipValue );
        RelationshipValue relationshipValue = (RelationshipValue) value;
        assertEquals( "TYPE_1", relationshipValue.type().stringValue() );

        MapValue properties = relationshipValue.properties();
        assertEquals( "Hello", ((StringValue) properties.get( "key1" )).stringValue() );

        ListValue list = (ListValue) properties.get( "arrayKey" );
        assertEquals( "a", ((StringValue) list.value( 0 )).stringValue() );
        assertEquals( "b", ((StringValue) list.value( 1 )).stringValue() );

        assertNotNull( relationshipValue.startNode() );
        assertNotNull( relationshipValue.endNode() );
    }

    @Test
    void testRelationshipWithSourceTag()
    {
        Value n = execute( "CREATE (n1) - [r:TYPE_1 {key1: 'Hello', arrayKey: ['a', 'b' ]}] -> (n2) RETURN r" );
        RecordConverter c1 = new RecordConverter( 1 );
        RecordConverter c2 = new RecordConverter( 2 );
        RelationshipValue r1a = (RelationshipValue) c1.convertValue( n );
        RelationshipValue r1b = (RelationshipValue) c1.convertValue( n );
        RelationshipValue r2 = (RelationshipValue) c2.convertValue( n );
        assertEquals( r1a, r1b );
        assertEquals( r1a.startNode(), r1b.startNode() );
        assertEquals( r1a.endNode(), r1b.endNode() );
        assertNotEquals( r1a, r2 );
        assertNotEquals( r1a.startNode(), r2.startNode() );
        assertNotEquals( r1a.endNode(), r2.endNode() );
        assertNotEquals( r1b, r2 );
        assertNotEquals( r1b.startNode(), r2.startNode() );
        assertNotEquals( r1b.endNode(), r2.endNode() );
    }

    @Test
    void testPath()
    {
        String query = "CREATE (n1:LABEL_1), (n2:LABEL_2), (n3:LABEL_3)\n" + "MERGE (n1) - [:TYPE_1] -> (n2)\n" + "MERGE (n2) - [:TYPE_2] -> (n3)\n" +
                "RETURN () - [] -> () - [] -> ()[0]";
        AnyValue value = executeAndConvert( query );
        assertTrue( value instanceof PathValue );
        PathValue pathValue = (PathValue) value;

        assertEquals( 2, pathValue.size() );
        NodeValue[] nodes = pathValue.nodes();

        assertEquals( "LABEL_1", nodes[0].labels().stringValue( 0 ) );
        assertEquals( "LABEL_2", nodes[1].labels().stringValue( 0 ) );
        assertEquals( "LABEL_3", nodes[2].labels().stringValue( 0 ) );

        RelationshipValue[] relationships = pathValue.relationships();
        RelationshipValue r1 = relationships[0];
        assertEquals( "TYPE_1", r1.type().stringValue() );
        assertEquals( "LABEL_1", r1.startNode().labels().stringValue( 0 ) );
        assertEquals( "LABEL_2", r1.endNode().labels().stringValue( 0 ) );

        RelationshipValue r2 = relationships[1];
        assertEquals( "TYPE_2", r2.type().stringValue() );
        assertEquals( "LABEL_2", r2.startNode().labels().stringValue( 0 ) );
        assertEquals( "LABEL_3", r2.endNode().labels().stringValue( 0 ) );
    }

    @Test
    void testDate()
    {
        AnyValue value = executeAndConvert( "RETURN date('2019-04-11')" );
        assertTrue( value instanceof DateValue );
        assertEquals( LocalDate.parse( "2019-04-11" ), ((DateValue) value).asObjectCopy() );
    }

    @Test
    void testLocalTime()
    {
        AnyValue value = executeAndConvert( "RETURN localtime('11:55')" );
        assertTrue( value instanceof LocalTimeValue );
        assertEquals( LocalTime.parse( "11:55" ), ((LocalTimeValue) value).asObjectCopy() );
    }

    @Test
    void testLocalDateTime()
    {
        AnyValue value = executeAndConvert( "RETURN localdatetime('2019-04-11T11:55')" );
        assertTrue( value instanceof LocalDateTimeValue );
        assertEquals( LocalDateTime.parse( "2019-04-11T11:55" ), ((LocalDateTimeValue) value).asObjectCopy() );
    }

    @Test
    void testTime()
    {
        AnyValue value = executeAndConvert( "RETURN time('12:50:35.556+01:00')" );
        assertTrue( value instanceof TimeValue );
        assertEquals( OffsetTime.parse( "12:50:35.556+01:00" ), ((TimeValue) value).asObjectCopy() );
    }

    @Test
    void testDateTime()
    {
        AnyValue value = executeAndConvert( "RETURN datetime('2015-06-24T12:50:35.556+01:00')" );
        assertTrue( value instanceof DateTimeValue );
        assertEquals( ZonedDateTime.parse( "2015-06-24T12:50:35.556+01:00" ), ((DateTimeValue) value).asObjectCopy() );
    }

    @Test
    void testDuration()
    {
        AnyValue value = executeAndConvert( "RETURN duration('P3M14DT12S')" );
        assertTrue( value instanceof DurationValue );
        TemporalAmount temporalAmount = ((DurationValue) value).asObjectCopy();
        assertEquals( 3, temporalAmount.get( ChronoUnit.MONTHS ) );
        assertEquals( 14, temporalAmount.get( ChronoUnit.DAYS ) );
        assertEquals( 12, temporalAmount.get( ChronoUnit.SECONDS ) );
        assertEquals( 0, temporalAmount.get( ChronoUnit.NANOS ) );
    }

    @Test
    void testCartesian2DPoint()
    {
        AnyValue value = executeAndConvert( "RETURN point({x: 1.0, y: 2.0, crs: 'cartesian'})" );
        assertTrue( value instanceof PointValue );
        PointValue pointValue = (PointValue) value;
        assertArrayEquals( new double[]{1.0, 2.0}, pointValue.coordinate(), 0.1 );
        CoordinateReferenceSystem csr = pointValue.getCoordinateReferenceSystem();
        assertEquals( "cartesian", csr.getName() );
    }

    @Test
    void testCartesian3DPoint()
    {
        AnyValue value = executeAndConvert( "RETURN point({x: 1.0, y: 2.0, z: 3.0, crs: 'cartesian-3d'})" );
        assertTrue( value instanceof PointValue );
        PointValue pointValue = (PointValue) value;
        assertArrayEquals( new double[]{1.0, 2.0, 3.0}, pointValue.coordinate(), 0.1 );
        CoordinateReferenceSystem csr = pointValue.getCoordinateReferenceSystem();
        assertEquals( "cartesian-3d", csr.getName() );
    }

    @Test
    void testWsg2DPoint()
    {
        AnyValue value = executeAndConvert( "RETURN point({x: 1.0, y: 2.0, crs: 'wgs-84'})" );
        assertTrue( value instanceof PointValue );
        PointValue pointValue = (PointValue) value;
        assertArrayEquals( new double[]{1.0, 2.0}, pointValue.coordinate(), 0.1 );
        CoordinateReferenceSystem csr = pointValue.getCoordinateReferenceSystem();
        assertEquals( "wgs-84", csr.getName() );
    }

    @Test
    void testWsg3DPoint()
    {
        AnyValue value = executeAndConvert( "RETURN point({x: 1.0, y: 2.0, z: 3.0, crs: 'wgs-84-3d'})" );
        assertTrue( value instanceof PointValue );
        PointValue pointValue = (PointValue) value;
        assertArrayEquals( new double[]{1.0, 2.0, 3.0}, pointValue.coordinate(), 0.1 );
        CoordinateReferenceSystem csr = pointValue.getCoordinateReferenceSystem();
        assertEquals( "wgs-84-3d", csr.getName() );
    }

    private AnyValue executeAndConvert( String query )
    {
        return executeAndConvert( query, Collections.emptyMap() );
    }

    private AnyValue executeAndConvert( String query, Map<String,Object> params )
    {
        return converter.convertValue( execute( query, params ) );
    }

    private Value execute( String query )
    {
        return execute( query, Collections.emptyMap() );
    }

    private Value execute( String query, Map<String,Object> params )
    {
        try ( Session session = driver.session() )
        {
            Result result = session.run( query, params );
            return result.list().get( 0 ).get( 0 );
        }
    }
}
