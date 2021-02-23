/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.Value;
import org.neo4j.driver.types.IsoDuration;
import org.neo4j.driver.types.MapAccessor;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Point;
import org.neo4j.driver.types.Relationship;
import org.neo4j.fabric.executor.ExecutionOptions;
import org.neo4j.fabric.stream.SourceTagging;
import org.neo4j.internal.helpers.collection.Iterables;
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
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.neo4j.values.storable.Values.NO_VALUE;

class RecordConverter
{

    private final boolean hasSourceTag;
    private final long sourceTagValue;

    RecordConverter()
    {
        this.hasSourceTag = false;
        this.sourceTagValue = 0;
    }

    RecordConverter( ExecutionOptions options )
    {
        this.hasSourceTag = options.addSourceTag();

        if ( hasSourceTag )
        {
            this.sourceTagValue = SourceTagging.makeSourceTag( options.sourceId() );
        }
        else
        {
            this.sourceTagValue = 0;
        }
    }

    AnyValue convertValue( Value driverValue )
    {
        Object value = driverValue.asObject();
        return convertValue( value );
    }

    private AnyValue convertValue( Object driverValue )
    {
        if ( driverValue == null )
        {
            return NO_VALUE;
        }

        if ( driverValue instanceof Node )
        {
            return convertNode( (Node) driverValue );
        }

        if ( driverValue instanceof Relationship )
        {
            return convertRelationship( (Relationship) driverValue );
        }

        if ( driverValue instanceof Path )
        {
            return convertPath( (Path) driverValue );
        }

        if ( driverValue instanceof Long )
        {
            return convertLong( (Long) driverValue );
        }

        if ( driverValue instanceof Double )
        {
            return convertDouble( (Double) driverValue );
        }

        if ( driverValue instanceof Boolean )
        {
            return convertBoolean( (Boolean) driverValue );
        }

        if ( driverValue instanceof String )
        {
            return convertString( (String) driverValue );
        }

        if ( driverValue instanceof Map )
        {
            return convertMap( (Map<String,Object>) driverValue );
        }

        if ( driverValue instanceof List )
        {
            return convertList( (List<Object>) driverValue );
        }

        if ( driverValue instanceof byte[] )
        {
            return convertBytes( (byte[]) driverValue );
        }

        if ( driverValue instanceof LocalDate )
        {
            return convertDate( (LocalDate) driverValue );
        }

        if ( driverValue instanceof LocalTime )
        {
            return convertLocalTime( (LocalTime) driverValue );
        }

        if ( driverValue instanceof LocalDateTime )
        {
            return convertLocalDateTime( (LocalDateTime) driverValue );
        }

        if ( driverValue instanceof OffsetTime )
        {
            return convertTime( (OffsetTime) driverValue );
        }

        if ( driverValue instanceof ZonedDateTime )
        {
            return convertDateTime( (ZonedDateTime) driverValue );
        }

        if ( driverValue instanceof IsoDuration )
        {
            return convertDuration( (IsoDuration) driverValue );
        }

        if ( driverValue instanceof Point )
        {
            return convertPoint( (Point) driverValue );
        }

        throw new IllegalStateException( "Unsupported type encountered: " + driverValue.getClass() );
    }

    private LongValue convertLong( Long driverValue )
    {
        return Values.longValue( driverValue );
    }

    private DoubleValue convertDouble( Double driverValue )
    {
        return Values.doubleValue( driverValue );
    }

    private BooleanValue convertBoolean( Boolean driverValue )
    {
        return Values.booleanValue( driverValue );
    }

    private ByteArray convertBytes( byte[] driverValue )
    {
        return Values.byteArray( driverValue );
    }

    private long convertId( long driverValue )
    {
        if ( hasSourceTag )
        {
            return SourceTagging.tagId( driverValue, sourceTagValue );
        }
        else
        {
            return driverValue;
        }
    }

    private NodeValue convertNode( Node driverValue )
    {
        String[] labels = Iterables.asArray( String.class, driverValue.labels() );
        TextArray serverLabels = convertStringArray( labels );
        MapValue properties = convertMap( driverValue );

        return VirtualValues.nodeValue( convertId( driverValue.id() ), serverLabels, properties );
    }

    private RelationshipValue convertRelationship( Relationship driverValue )
    {
        NodeValue startNode = convertNodeReference( convertId( driverValue.startNodeId() ) );
        NodeValue endNode = convertNodeReference( convertId( driverValue.endNodeId() ) );
        return convertRelationship( driverValue, startNode, endNode );
    }

    private RelationshipValue convertRelationship( Relationship driverValue, NodeValue startNode, NodeValue endNode )
    {
        TextValue type = convertString( driverValue.type() );
        MapValue properties = convertMap( driverValue );

        return VirtualValues.relationshipValue( convertId( driverValue.id() ), startNode, endNode, type, properties );
    }

    private MapValue convertMap( MapAccessor driverValue )
    {
        if ( driverValue.size() == 0 )
        {
            return VirtualValues.EMPTY_MAP;
        }
        MapValueBuilder builder = new MapValueBuilder( driverValue.size() );
        for ( String key : driverValue.keys() )
        {
            builder.add( key, convertValue( driverValue.get( key ) ) );
        }

        return builder.build();
    }

    private PathValue convertPath( Path driverValue )
    {
        Map<Long,NodeValue> encounteredNodes = new HashMap<>( driverValue.length() + 1 );
        NodeValue[] nodes = new NodeValue[driverValue.length() + 1];
        RelationshipValue[] relationships = new RelationshipValue[driverValue.length()];

        int i = 0;
        for ( Node driverNode : driverValue.nodes() )
        {
            NodeValue node = convertNode( driverNode );
            nodes[i] = node;
            encounteredNodes.put( driverNode.id(), node );
            i++;
        }

        i = 0;

        for ( Relationship driverRelationship : driverValue.relationships() )
        {
            NodeValue startNode = encounteredNodes.get( driverRelationship.startNodeId() );
            NodeValue endNode = encounteredNodes.get( driverRelationship.endNodeId() );

            RelationshipValue relationship = convertRelationship( driverRelationship, startNode, endNode );
            relationships[i] = relationship;
            i++;
        }

        return VirtualValues.path( nodes, relationships );
    }

    private NodeValue convertNodeReference( long nodeId )
    {
        // return VirtualValues.node( nodeId );
        return VirtualValues.nodeValue( nodeId, convertStringArray( new String[0] ), convertMap( Collections.emptyMap() ) );
    }

    private TextArray convertStringArray( String[] driverValue )
    {
        return Values.stringArray( driverValue );
    }

    private MapValue convertMap( Map<String,Object> driverValue )
    {
        if ( driverValue.isEmpty() )
        {
            return VirtualValues.EMPTY_MAP;
        }
        MapValueBuilder builder = new MapValueBuilder( driverValue.size() );
        driverValue.forEach( ( key, value ) -> builder.add( key, convertValue( value ) ) );
        return builder.build();
    }

    private ListValue convertList( List<Object> driverValue )
    {
        ListValueBuilder builder = ListValueBuilder.newListBuilder( driverValue.size() );
        for ( Object o : driverValue )
        {
            builder.add( convertValue( o ) );
        }

        return builder.build();
    }

    private DateValue convertDate( LocalDate driverValue )
    {
        return DateValue.date( driverValue );
    }

    private LocalTimeValue convertLocalTime( LocalTime driverValue )
    {
        return LocalTimeValue.localTime( driverValue );
    }

    private LocalDateTimeValue convertLocalDateTime( LocalDateTime driverValue )
    {
        return LocalDateTimeValue.localDateTime( driverValue );
    }

    private TimeValue convertTime( OffsetTime driverValue )
    {
        return TimeValue.time( driverValue );
    }

    private DateTimeValue convertDateTime( ZonedDateTime driverValue )
    {
        return DateTimeValue.datetime( driverValue );
    }

    private DurationValue convertDuration( IsoDuration driverValue )
    {
        return DurationValue.duration( driverValue.months(), driverValue.days(), driverValue.seconds(), driverValue.nanoseconds() );
    }

    private PointValue convertPoint( Point point )
    {
        CoordinateReferenceSystem coordinateReferenceSystem = CoordinateReferenceSystem.get( point.srid() );

        if ( Double.isNaN( point.z() ) )
        {
            return Values.pointValue( coordinateReferenceSystem, point.x(), point.y() );
        }
        return Values.pointValue( coordinateReferenceSystem, point.x(), point.y(), point.z() );
    }

    private TextValue convertString( String driverValue )
    {
        return Values.utf8Value( driverValue );
    }
}
