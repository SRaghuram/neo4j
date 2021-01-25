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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteArray;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

class ParameterConverter
{
    Object convertValue( AnyValue serverValue )
    {
        if ( serverValue instanceof NoValue )
        {
            return null;
        }

        if ( serverValue instanceof LongValue )
        {
            return convertLong( (LongValue) serverValue );
        }

        if ( serverValue instanceof DoubleValue )
        {
            return convertDouble( (DoubleValue) serverValue );
        }

        if ( serverValue instanceof BooleanValue )
        {
            return convertBoolean( (BooleanValue) serverValue );
        }

        if ( serverValue instanceof StringValue )
        {
            return convertString( (StringValue) serverValue );
        }

        if ( serverValue instanceof MapValue )
        {
            return convertMap( (MapValue) serverValue );
        }

        if ( serverValue instanceof ListValue )
        {
            return convertList( (ListValue) serverValue );
        }

        if ( serverValue instanceof ByteArray )
        {
            return convertBytes( (ByteArray) serverValue );
        }

        if ( serverValue instanceof DateValue )
        {
            return convertDate( (DateValue) serverValue );
        }

        if ( serverValue instanceof LocalTimeValue )
        {
            return convertLocalTime( (LocalTimeValue) serverValue );
        }

        if ( serverValue instanceof LocalDateTimeValue )
        {
            return convertLocalDateTime( (LocalDateTimeValue) serverValue );
        }

        if ( serverValue instanceof TimeValue )
        {
            return convertTime( (TimeValue) serverValue );
        }

        if ( serverValue instanceof DateTimeValue )
        {
            return convertDateTime( (DateTimeValue) serverValue );
        }

        if ( serverValue instanceof DurationValue )
        {
            return convertDuration( (DurationValue) serverValue );
        }

        if ( serverValue instanceof PointValue )
        {
            return convertPoint( (PointValue) serverValue );
        }

        throw new IllegalStateException( "Unsupported type encountered: " + serverValue.getClass() );
    }

    private Long convertLong( LongValue serverValue )
    {
        return serverValue.asObjectCopy();
    }

    private Double convertDouble( DoubleValue serverValue )
    {
        return serverValue.asObjectCopy();
    }

    private boolean convertBoolean( BooleanValue serverValue )
    {
        return serverValue.booleanValue();
    }

    private byte[] convertBytes( ByteArray serverValue )
    {
        return serverValue.asObjectCopy();
    }

    private Map<String,Object> convertMap( MapValue serverValue )
    {
        Map<String,Object> driverValue = new HashMap<>();
        serverValue.foreach( ( key, value ) -> driverValue.put( key, convertValue( value ) ) );
        return driverValue;
    }

    private List<Object> convertList( ListValue serverValue )
    {
        List<Object> driverValue = new ArrayList<>();
        serverValue.forEach( value -> driverValue.add( convertValue( value ) ) );
        return driverValue;
    }

    private LocalDate convertDate( DateValue serverValue )
    {
        return serverValue.asObjectCopy();
    }

    private LocalTime convertLocalTime( LocalTimeValue serverValue )
    {
        return serverValue.asObjectCopy();
    }

    private LocalDateTime convertLocalDateTime( LocalDateTimeValue serverValue )
    {
        return serverValue.asObjectCopy();
    }

    private OffsetTime convertTime( TimeValue serverValue )
    {
        return serverValue.asObjectCopy();
    }

    private ZonedDateTime convertDateTime( DateTimeValue serverValue )
    {
        return serverValue.asObjectCopy();
    }

    private Value convertDuration( DurationValue serverValue )
    {
        return Values.isoDuration( serverValue.get( ChronoUnit.MONTHS ), serverValue.get( ChronoUnit.DAYS ), serverValue.get( ChronoUnit.SECONDS ),
                (int) serverValue.get( ChronoUnit.NANOS ) );
    }

    private Value convertPoint( PointValue point )
    {
        double[] coordinate = point.coordinate();
        CRS crs = point.getCRS();

        if ( coordinate.length == 2 )
        {
            return Values.point( crs.getCode(), coordinate[0], coordinate[1] );
        }

        return Values.point( crs.getCode(), coordinate[0], coordinate[1], coordinate[2] );
    }

    private String convertString( TextValue serverValue )
    {
        return serverValue.stringValue();
    }
}
