/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.data;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.stream.IntStream;

import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static com.neo4j.bench.data.ArrayGenerator.DEFAULT_SIZE;
import static com.neo4j.bench.data.ArrayGenerator.byteArray;
import static com.neo4j.bench.data.ArrayGenerator.doubleArray;
import static com.neo4j.bench.data.ArrayGenerator.floatArray;
import static com.neo4j.bench.data.ArrayGenerator.intArray;
import static com.neo4j.bench.data.ArrayGenerator.longArray;
import static com.neo4j.bench.data.ArrayGenerator.shortArray;
import static com.neo4j.bench.data.ArrayGenerator.stringArray;
import static com.neo4j.bench.data.StringGenerator.BIG_STRING_LENGTH;
import static com.neo4j.bench.data.StringGenerator.SMALL_STRING_LENGTH;
import static com.neo4j.bench.data.ValueGeneratorUtil.BYTE;
import static com.neo4j.bench.data.ValueGeneratorUtil.BYTE_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.DATE;
import static com.neo4j.bench.data.ValueGeneratorUtil.DATE_TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.DBL;
import static com.neo4j.bench.data.ValueGeneratorUtil.DBL_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.DURATION;
import static com.neo4j.bench.data.ValueGeneratorUtil.FLT;
import static com.neo4j.bench.data.ValueGeneratorUtil.FLT_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.INT;
import static com.neo4j.bench.data.ValueGeneratorUtil.INT_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.data.ValueGeneratorUtil.LNG_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.LOCAL_DATE_TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.LOCAL_TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.POINT;
import static com.neo4j.bench.data.ValueGeneratorUtil.SHORT;
import static com.neo4j.bench.data.ValueGeneratorUtil.SHORT_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_BIG;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_BIG_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_INL;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_SML_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.TIME;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.stream.Collectors.joining;

public class ConstantGenerator
{
    public static ConstantGeneratorFactory constant( String type, Object constant )
    {
        switch ( type )
        {
        case BYTE:
        case BYTE_ARR:
        case SHORT:
        case SHORT_ARR:
        case INT:
        case INT_ARR:
        case STR_INL:
        case STR_SML:
        case STR_SML_ARR:
        case STR_BIG:
        case STR_BIG_ARR:
        case LNG:
        case LNG_ARR:
        case DATE_TIME:
        case LOCAL_DATE_TIME:
        case TIME:
        case LOCAL_TIME:
        case DATE:
        case DURATION:
        case FLT:
        case FLT_ARR:
        case DBL:
        case DBL_ARR:
        case POINT:
            return new ConstantGeneratorFactory( type, constant );
        default:
            throw new RuntimeException( "Unsupported type: " + type );
        }
    }

    public static final String BIG_STRING_PREFIX = IntStream.range( 0, BIG_STRING_LENGTH )
                                                      .mapToObj( i -> Integer.toString( i ).substring( 0, 1 ) )
                                                      .collect( joining() );

    public static class ConstantGeneratorFactory implements ValueGeneratorFactory
    {
        private String type;
        private Object constant;

        public ConstantGeneratorFactory()
        {
        }

        public ConstantGeneratorFactory( String type, Object constant )
        {
            this.type = type;
            this.constant = constant;
        }

        @Override
        public ValueGeneratorFun<Object> create()
        {
            Object value = getValue( type, constant );
            Value constantValue = Values.of( value );

            return new ValueGeneratorFun<>()
            {
                @Override
                public boolean wrapped()
                {
                    return false;
                }

                @Override
                public Object next( SplittableRandom rng )
                {
                    return value;
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return constantValue;
                }
            };
        }

        private static Object getValue( String type, Object constant )
        {
            switch ( type )
            {
            case BYTE:
                return asNumber( constant ).byteValue();
            case SHORT:
                return asNumber( constant ).shortValue();
            case INT:
                return asNumber( constant ).intValue();
            case LNG:
                return asNumber( constant ).longValue();
            case FLT:
                return asNumber( constant ).floatValue();
            case DBL:
                return asNumber( constant ).doubleValue();
            case STR_INL:
            case STR_SML:
                return isNumber( constant )
                       ? Integer.toString( asNumber( constant ).intValue() )
                       : asString( type, constant );
            case STR_BIG:
                return isNumber( constant )
                       ? BIG_STRING_PREFIX + asNumber( constant ).intValue()
                       : asString( type, constant );
            case POINT:
                return Values.pointValue( CoordinateReferenceSystem.Cartesian, asNumber( constant ).doubleValue(), asNumber( constant ).doubleValue() );
            case DATE_TIME:
                return DateTimeValue.datetime( asNumber( constant ).longValue(), 0, UTC ).asObjectCopy();
            case LOCAL_DATE_TIME:
                return LocalDateTimeValue.localDateTime( asNumber( constant ).longValue(), 0 ).asObjectCopy();
            case TIME:
                return TimeValue.time( asNumber( constant ).longValue(), UTC ).asObjectCopy();
            case LOCAL_TIME:
                return LocalTimeValue.localTime( asNumber( constant ).longValue() ).asObjectCopy();
            case DATE:
                return DateValue.epochDate( asNumber( constant ).longValue() ).asObjectCopy();
            case DURATION:
                return DurationValue.duration( 0, 0, 0, asNumber( constant ).longValue() ).asObjectCopy();
            case BYTE_ARR:
                // NOTE: it is safe for rng to be null, inner constant generator should not be using it
                return byteArray( constant( BYTE, asNumber( constant ) ), DEFAULT_SIZE ).create().next( null );
            case SHORT_ARR:
                // NOTE: it is safe for rng to be null, inner constant generator should not be using it
                return shortArray( constant( SHORT, asNumber( constant ) ), DEFAULT_SIZE ).create().next( null );
            case INT_ARR:
                // NOTE: it is safe for rng to be null, inner constant generator should not be using it
                return intArray( constant( INT, asNumber( constant ) ), DEFAULT_SIZE ).create().next( null );
            case LNG_ARR:
                // NOTE: it is safe for rng to be null, inner constant generator should not be using it
                return longArray( constant( LNG, asNumber( constant ) ), DEFAULT_SIZE ).create().next( null );
            case FLT_ARR:
                // NOTE: it is safe for rng to be null, inner constant generator should not be using it
                return floatArray( constant( FLT, asNumber( constant ) ), DEFAULT_SIZE ).create().next( null );
            case DBL_ARR:
                // NOTE: it is safe for rng to be null, inner constant generator should not be using it
                return doubleArray( constant( DBL, asNumber( constant ) ), DEFAULT_SIZE ).create().next( null );
            case STR_SML_ARR:
                // NOTE: it is safe for rng to be null, inner constant generator should not be using it
                return stringArray( constant( STR_SML, constant ), DEFAULT_SIZE ).create().next( null );
            case STR_BIG_ARR:
                // NOTE: it is safe for rng to be null, inner constant generator should not be using it
                return stringArray( constant( STR_BIG, constant ), DEFAULT_SIZE ).create().next( null );
            default:
                throw new RuntimeException( "Unsupported type: " + type );
            }
        }

        private static String asString( String type, Object maybeString )
        {
            Class<?> maybeStringClass = maybeString.getClass();
            if ( !String.class.equals( maybeStringClass ) )
            {
                throw new RuntimeException( format( "Expected '%s' to be a string but was: %s", maybeString, maybeStringClass.getName() ) );
            }
            String string = (String) maybeString;
            List<String> validTypes = Arrays.asList( STR_INL, STR_SML, STR_BIG );
            if ( !validTypes.contains( type ) )
            {
                throw new RuntimeException( format( "Expected '%s' to be a valid string type %s but was: %s", string, validTypes, type ) );
            }
            if ( type.equals( STR_BIG ) )
            {
                // Maybe this test is too strict? Let's see how it goes.
                if ( string.length() < BIG_STRING_LENGTH )
                {
                    throw new RuntimeException(
                            format( "Expected '%s':%s to have length >= %s but was: %s", string, type, BIG_STRING_LENGTH, string.length() ) );
                }
            }
            else
            {
                // Maybe this test is too strict? Let's see how it goes.
                if ( string.length() > SMALL_STRING_LENGTH )
                {
                    throw new RuntimeException(
                            format( "Expected '%s':%s to have length <= %s but was: %s", string, type, SMALL_STRING_LENGTH, string.length() ) );
                }
            }
            return string;
        }

        private static Number asNumber( Object maybeNumber )
        {
            if ( !isNumber( maybeNumber ) )
            {
                throw new RuntimeException( format( "Expected '%s' to be a number but was: %s", maybeNumber, maybeNumber.getClass().getName() ) );
            }
            return (Number) maybeNumber;
        }

        private static boolean isNumber( Object maybeNumber )
        {
            return Number.class.isAssignableFrom( maybeNumber.getClass() );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            ConstantGeneratorFactory that = (ConstantGeneratorFactory) o;
            return Objects.equals( type, that.type ) &&
                   Objects.equals( constant, that.constant );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( type, constant );
        }
    }
}
