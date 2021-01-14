/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.data;

import com.neo4j.bench.data.CRS.Cartesian;
import com.neo4j.bench.data.DiscreteGenerator.Bucket;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static com.neo4j.bench.data.ArrayGenerator.DEFAULT_SIZE;
import static com.neo4j.bench.data.ArrayGenerator.byteArray;
import static com.neo4j.bench.data.ArrayGenerator.doubleArray;
import static com.neo4j.bench.data.ArrayGenerator.floatArray;
import static com.neo4j.bench.data.ArrayGenerator.intArray;
import static com.neo4j.bench.data.ArrayGenerator.longArray;
import static com.neo4j.bench.data.ArrayGenerator.shortArray;
import static com.neo4j.bench.data.ArrayGenerator.stringArray;
import static com.neo4j.bench.data.DiscreteGenerator.discrete;
import static com.neo4j.bench.data.NumberGenerator.ascByte;
import static com.neo4j.bench.data.NumberGenerator.ascDouble;
import static com.neo4j.bench.data.NumberGenerator.ascFloat;
import static com.neo4j.bench.data.NumberGenerator.ascInt;
import static com.neo4j.bench.data.NumberGenerator.ascLong;
import static com.neo4j.bench.data.NumberGenerator.ascShort;
import static com.neo4j.bench.data.NumberGenerator.randByte;
import static com.neo4j.bench.data.NumberGenerator.randDouble;
import static com.neo4j.bench.data.NumberGenerator.randFloat;
import static com.neo4j.bench.data.NumberGenerator.randInt;
import static com.neo4j.bench.data.NumberGenerator.randLong;
import static com.neo4j.bench.data.NumberGenerator.randShort;
import static com.neo4j.bench.data.NumberGenerator.stridingDouble;
import static com.neo4j.bench.data.NumberGenerator.stridingFloat;
import static com.neo4j.bench.data.NumberGenerator.stridingInt;
import static com.neo4j.bench.data.NumberGenerator.stridingLong;
import static com.neo4j.bench.data.NumberGenerator.toDouble;
import static com.neo4j.bench.data.NumberGenerator.toFloat;
import static com.neo4j.bench.data.PointGenerator.diagonal;
import static com.neo4j.bench.data.PointGenerator.grid;
import static com.neo4j.bench.data.StringGenerator.BIG_STRING_LENGTH;
import static com.neo4j.bench.data.StringGenerator.SMALL_STRING_LENGTH;
import static com.neo4j.bench.data.StringGenerator.intString;
import static com.neo4j.bench.data.StringGenerator.leftPaddingFunFor;
import static com.neo4j.bench.data.StringGenerator.randInlinedAlphaNumerical;
import static com.neo4j.bench.data.StringGenerator.randShortUtf8;
import static com.neo4j.bench.data.StringGenerator.randUtf8;
import static com.neo4j.bench.data.TemporalGenerator.date;
import static com.neo4j.bench.data.TemporalGenerator.dateTime;
import static com.neo4j.bench.data.TemporalGenerator.duration;
import static com.neo4j.bench.data.TemporalGenerator.localDatetime;
import static com.neo4j.bench.data.TemporalGenerator.localTime;
import static com.neo4j.bench.data.TemporalGenerator.time;

import static java.lang.Math.round;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;

public class ValueGeneratorUtil
{
    public static final String BYTE = "byte";
    public static final String SHORT = "short";
    public static final String INT = "int";
    public static final String LNG = "long";
    public static final String FLT = "float";
    public static final String DBL = "double";
    public static final String STR_INL = "inlined_string";
    public static final String STR_SML = "small_string";
    public static final String STR_BIG = "big_string";
    public static final String POINT = "point";
    public static final String DATE_TIME = "date_time";
    public static final String LOCAL_DATE_TIME = "local_date_time";
    public static final String TIME = "time";
    public static final String LOCAL_TIME = "local_time";
    public static final String DATE = "date";
    public static final String DURATION = "duration";
    public static final String BYTE_ARR = "byte[]";
    public static final String SHORT_ARR = "short[]";
    public static final String INT_ARR = "int[]";
    public static final String LNG_ARR = "long[]";
    public static final String FLT_ARR = "float[]";
    public static final String DBL_ARR = "double[]";
    public static final String STR_SML_ARR = "small_string[]";
    public static final String STR_BIG_ARR = "big_string[]";

    /*
    We create DateTimes from epochSecond
    The valid Range of years is from -1,000,000,000 BC to 1,000,000,000 AD (exclusive)
    As epochSecond of 0 means 1970 AD, we need to offset the below calculation by 1970 years to get the right range in seconds
    */
    private static final long MAX_ALLOWED_YEARS = 999_999_999;
    private static final long START_OF_EPOCH = 1970;
    private static final long DAYS_PER_YEAR = 365;
    private static final long TIME_RANGE_MIN = 0;
    private static final long TIME_RANGE_MAX = HOURS.toNanos( 24 ) - 1;
    private static final long DATE_TIME_RANGE_MIN_SECONDS = DAYS.toSeconds( (-MAX_ALLOWED_YEARS - START_OF_EPOCH) * DAYS_PER_YEAR );
    private static final long DATE_TIME_RANGE_MAX_SECONDS = DAYS.toSeconds( (MAX_ALLOWED_YEARS - START_OF_EPOCH) * DAYS_PER_YEAR );
    private static final long DATE_RANGE_MIN_DAYS = (-MAX_ALLOWED_YEARS - START_OF_EPOCH) * DAYS_PER_YEAR;
    private static final long DATE_RANGE_MAX_DAYS = (MAX_ALLOWED_YEARS - START_OF_EPOCH) * DAYS_PER_YEAR;

    /**
     * Calculates default/suggested min and max values, by type, for random number generators
     *
     * @param type
     * @return
     */
    public static Range defaultRangeFor( String type )
    {
        switch ( type )
        {
        case BYTE:
            return new Range( 0, Byte.MAX_VALUE );
        case SHORT:
            return new Range( 0, Short.MAX_VALUE );
        case INT:
            return new Range( 0, Integer.MAX_VALUE );
        case LNG:
            return new Range( Integer.MAX_VALUE, Long.MAX_VALUE );
        case FLT:
            return new Range( 0, Float.MAX_VALUE );
        case DBL:
            return new Range( 0, Double.MAX_VALUE );
        case STR_INL:
            return new Range( 0, Long.MAX_VALUE );
        case STR_SML:
            return new Range( 0, Long.MAX_VALUE );
        case STR_BIG:
            return new Range( 0, Long.MAX_VALUE );
        case POINT:
            // TODO revise?
            return new Range( -1_000_000D, 1_000_000D );
        case DATE_TIME:
            return new Range( DATE_TIME_RANGE_MIN_SECONDS, DATE_TIME_RANGE_MAX_SECONDS );
        case LOCAL_DATE_TIME:
            return new Range( DATE_TIME_RANGE_MIN_SECONDS, DATE_TIME_RANGE_MAX_SECONDS );
        case TIME:
            return new Range( TIME_RANGE_MIN, TIME_RANGE_MAX );
        case LOCAL_TIME:
            return new Range( TIME_RANGE_MIN, TIME_RANGE_MAX );
        case DATE:
            return new Range( DATE_RANGE_MIN_DAYS, DATE_RANGE_MAX_DAYS );
        case DURATION:
            return defaultRangeFor( LNG );
        case BYTE_ARR:
            return defaultRangeFor( BYTE );
        case SHORT_ARR:
            return defaultRangeFor( SHORT );
        case INT_ARR:
            return defaultRangeFor( INT );
        case LNG_ARR:
            return defaultRangeFor( LNG );
        case FLT_ARR:
            return defaultRangeFor( FLT );
        case DBL_ARR:
            return defaultRangeFor( DBL );
        case STR_SML_ARR:
            return defaultRangeFor( LNG );
        case STR_BIG_ARR:
            return defaultRangeFor( LNG );
        default:
            throw new RuntimeException( "Unsupported type: " + type );
        }
    }

    public static class Range
    {
        private final Number min;
        private final Number max;

        private Range( Number min, Number max )
        {
            this.min = min;
            this.max = max;
        }

        public Number min()
        {
            return min;
        }

        public Number max()
        {
            return max;
        }
    }

    /**
     * Creates property definition with default random value generation policy for given type.
     *
     * @param type : type of values to generate
     * @return
     */
    public static PropertyDefinition randPropertyFor( String type )
    {
        return randPropertyFor( type, false );
    }

    /**
     * Creates property definition with default random value generation policy for given type. Property key will be given key.
     *
     * @param type : type of values to generate
     * @param key  : property key
     * @return
     */
    public static PropertyDefinition randPropertyFor( String type, String key )
    {
        return randPropertyFor( type, key, false );
    }

    /**
     * Creates property definition with default random value generation policy for given type. If numerical is true, string generators return string
     * representations of number sequences.
     *
     * @param type      : type of values to generate
     * @param numerical
     * @return
     */
    public static PropertyDefinition randPropertyFor( String type, boolean numerical )
    {
        return randPropertyFor( type, type, numerical );
    }

    /**
     * Creates property definition with default random value generation policy for given type. If numerical is true, string generators return string
     * representation of number sequences. Property key will be given key.
     *
     * @param type      : type of values to generate
     * @param key       : property key
     * @param numerical
     * @return
     */
    public static PropertyDefinition randPropertyFor( String type, String key, boolean numerical )
    {
        Range range = defaultRangeFor( type );
        ValueGeneratorFactory valueGeneratorFactory = randGeneratorFor( type, range.min(), range.max(), numerical );
        return new PropertyDefinition( key, valueGeneratorFactory );
    }

    /**
     * Creates property definition with random value generation policy for given type, and within given range. If numerical is true, string generators return
     * string representations of number sequences.
     *
     * @param type      : type of values to generate
     * @param min
     * @param max
     * @param numerical
     * @return
     */
    public static PropertyDefinition randPropertyFor( String type, Number min, Number max, boolean numerical )
    {
        return new PropertyDefinition( type, randGeneratorFor( type, min, max, numerical ) );
    }

    /**
     * Creates property definition with default ascending value generation policy for given type.
     *
     * @param type : type of values to generate
     * @return
     */
    public static PropertyDefinition ascPropertyFor( String type )
    {
        Range range = defaultRangeFor( type );
        ValueGeneratorFactory valueGeneratorFactory = ascGeneratorFor( type, range.min() );
        return new PropertyDefinition( type, valueGeneratorFactory );
    }

    /**
     * Creates property definition with default ascending value generation policy for given type, starting from initial value.
     *
     * @param type    : type of values to generate
     * @param initial : value from which to starting ascending
     * @return
     */
    public static PropertyDefinition ascPropertyFor( String type, Number initial )
    {
        return new PropertyDefinition( type, ascGeneratorFor( type, initial ) );
    }

    /**
     * Creates property definition with discrete value generation policy for given type. Creates buckets for creating discrete value generators.
     *
     * @param type   : type of values to generate
     * @param ratios : probabilities of selecting each bucket
     * @return buckets
     */
    public static Bucket[] discreteBucketsFor( String type, double... ratios )
    {
        return IntStream.range( 0, ratios.length )
                        .mapToObj( i -> new Bucket( ratios[i], ConstantGenerator.constant( type, i ) ) )
                        .toArray( Bucket[]::new );
    }

    public static ValueGeneratorFactory discreteFor( String type, double... ratios )
    {
        return discrete( discreteBucketsFor( type, ratios ) );
    }

    /**
     * Assumes input collection contains selectivities for data in a store. Assumes subsequent collection elements are selectivity of superset of all prior
     * elements. E.g., element x represents selectivity of string that is prefix of all strings represented by elements (y < x).
     *
     * @param selectivities
     * @return cumulative selectivities
     */
    public static List<Double> calculateCumulativeSelectivities( List<Double> selectivities )
    {
        if ( selectivities.size() <= 1 )
        {
            return selectivities;
        }
        // assert input is in ascending order
        for ( int i = 1; i < selectivities.size(); i++ )
        {
            if ( selectivities.get( i ) < selectivities.get( i - 1 ) )
            {
                throw new DataGeneratorException( "Input selectivities must be in ascending order" );
            }
        }
        // make copy of input, so it can be sorted without mutating the provided collection
        List<Double> originalSelectivities = new ArrayList<>( selectivities );
        Collections.sort( originalSelectivities );
        List<Double> cumulativeSelectivities = new ArrayList<>();
        cumulativeSelectivities.add( originalSelectivities.get( 0 ) );
        for ( int i = 1; i < originalSelectivities.size(); i++ )
        {
            Double correctedCumulativeSelectivity = makeSelectivityCumulative(
                    originalSelectivities.get( i ),
                    cumulativeSelectivities );
            cumulativeSelectivities.add( correctedCumulativeSelectivity );
        }
        return cumulativeSelectivities;
    }

    static Double makeSelectivityCumulative( Double targetSelectivity, List<Double> lowerSelectivities )
    {
        double lowerSelectivitiesSum = lowerSelectivities.stream().mapToDouble( d -> d ).sum();
        if ( lowerSelectivitiesSum >= targetSelectivity )
        {
            throw new DataGeneratorException( format( "Sum of lower selectivities (%s) must be lower than target selectivity (%s)",
                                                      lowerSelectivitiesSum,
                                                      targetSelectivity ) );
        }
        return targetSelectivity - lowerSelectivitiesSum;
    }

    public static Object asIntegral( String type, Number number )
    {
        switch ( type )
        {
        case BYTE:
        case SHORT:
        case INT:
        case LNG:
        case FLT:
        case DBL:
            return number;
        case STR_SML:
        case STR_BIG:
            return leftPaddingFunFor( stringLengthFor( type ) ).apply( number.intValue() );
        default:
            throw new RuntimeException( "Unsupported type: " + type );
        }
    }

    public static Integer stringLengthFor( String type )
    {
        switch ( type )
        {
        case STR_SML:
            return SMALL_STRING_LENGTH;
        case STR_BIG:
            return BIG_STRING_LENGTH;
        default:
            throw new RuntimeException( "Unsupported type: " + type );
        }
    }

    public static String middlePad( String original, Character character, int lengthAfterPadding )
    {
        assertSanePadLength( lengthAfterPadding, original );
        int paddingLength = lengthAfterPadding - original.length();
        int prefixLength = (int) round( Math.floor( paddingLength / 2D ) );
        int suffixLength = (int) round( Math.ceil( paddingLength / 2d ) );
        String withSuffix = suffixPad( original, character, suffixLength + original.length() );
        return prefixPad( withSuffix, character, prefixLength + original.length() + suffixLength );
    }

    public static String suffixPad( String original, Character character, int lengthAfterPadding )
    {
        assertSanePadLength( lengthAfterPadding, original );
        String suffix = padString( original.length() - lengthAfterPadding, character );
        return original + suffix;
    }

    public static String prefixPad( String original, Character character, int lengthAfterPadding )
    {
        assertSanePadLength( lengthAfterPadding, original );
        String prefix = padString( original.length() - lengthAfterPadding, character );
        return prefix + original;
    }

    private static void assertSanePadLength( int lengthAfterPadding, String original )
    {
        if ( lengthAfterPadding < original.length() )
        {
            throw new DataGeneratorException( "Distance after padding can not be shorter than before" );
        }
    }

    private static String padString( int length, Character character )
    {
        return (0 == length)
               ? ""
               : format( "%" + length + "s", "" ).replace( ' ', character );
    }

    /**
     * Creates random value generator for given type, and within given range. If numerical is true, string generators return string representations of number
     * sequences.
     *
     * @param type     : type of values to generate
     * @param min
     * @param max
     * @param integral
     * @return
     */
    public static ValueGeneratorFactory randGeneratorFor( String type, Number min, Number max, boolean integral )
    {
        switch ( type )
        {
        case BYTE:
            return randByte( min.byteValue(), max.byteValue() );
        case SHORT:
            return randShort( min.shortValue(), max.shortValue() );
        case INT:
            return randInt( min.intValue(), max.intValue() );
        case LNG:
            return randLong( min.longValue(), max.longValue() );
        case FLT:
            return integral ? toFloat( randGeneratorFor( INT, min, max, integral ) )
                            : randFloat( min.floatValue(), max.floatValue() );
        case DBL:
            return integral ? toDouble( randGeneratorFor( LNG, min, max, integral ) )
                            : randDouble( min.doubleValue(), max.doubleValue() );
        case STR_INL:
            return randInlinedAlphaNumerical();
        case STR_SML:
            return integral ? intString( randGeneratorFor( INT, min, max, integral ), SMALL_STRING_LENGTH )
                            : randShortUtf8();
        case STR_BIG:
            return integral ? intString( randGeneratorFor( INT, min, max, integral ), BIG_STRING_LENGTH )
                            : randUtf8( BIG_STRING_LENGTH );
        case POINT:
            return PointGenerator.random(
                    min.doubleValue(),
                    max.doubleValue(),
                    min.doubleValue(),
                    max.doubleValue(),
                    new Cartesian() );
        case DATE_TIME:
            return dateTime( randGeneratorFor( LNG, min, max, integral ) );
        case LOCAL_DATE_TIME:
            return localDatetime( randGeneratorFor( LNG, min, max, integral ) );
        case TIME:
            return time( randGeneratorFor( LNG, min, max, integral ) );
        case LOCAL_TIME:
            return localTime( randGeneratorFor( LNG, min, max, integral ) );
        case DATE:
            return date( randGeneratorFor( LNG, min, max, integral ) );
        case DURATION:
            return duration( randGeneratorFor( LNG, min, max, integral ) );
        case SHORT_ARR:
            return shortArray( randGeneratorFor( SHORT, min, max, integral ), DEFAULT_SIZE );
        case BYTE_ARR:
            return byteArray( randGeneratorFor( BYTE, min, max, integral ), DEFAULT_SIZE );
        case INT_ARR:
            return intArray( randGeneratorFor( INT, min, max, integral ), DEFAULT_SIZE );
        case LNG_ARR:
            return longArray( randGeneratorFor( LNG, min, max, integral ), DEFAULT_SIZE );
        case FLT_ARR:
            return floatArray( randGeneratorFor( FLT, min, max, integral ), DEFAULT_SIZE );
        case DBL_ARR:
            return doubleArray( randGeneratorFor( DBL, min, max, integral ), DEFAULT_SIZE );
        case STR_SML_ARR:
            return stringArray( randGeneratorFor( STR_SML, min, max, integral ), DEFAULT_SIZE );
        case STR_BIG_ARR:
            return stringArray( randGeneratorFor( STR_BIG, min, max, integral ), DEFAULT_SIZE );
        default:
            throw new RuntimeException( "Unsupported type: " + type );
        }
    }

    /**
     * Creates property definition with default ascending value generation policy for given type, starting from initial value.
     *
     * @param type    : type of values to generate
     * @param initial : value from which to starting ascending
     * @return
     */
    public static ValueGeneratorFactory ascGeneratorFor( String type, Number initial )
    {
        switch ( type )
        {
        case INT:
            return ascInt( initial.intValue() );
        case LNG:
            return ascLong( initial.longValue() );
        case FLT:
            return ascFloat( initial.floatValue() );
        case DBL:
            return ascDouble( initial.doubleValue() );
        case STR_INL:
        case STR_SML:
            return intString( ascInt( initial.intValue() ), SMALL_STRING_LENGTH );
        case STR_BIG:
            return intString( ascInt( initial.intValue() ), BIG_STRING_LENGTH );
        case POINT:
            Range range = defaultRangeFor( POINT );
            if ( range.max().doubleValue() <= initial.doubleValue() )
            {
                throw new RuntimeException( format( "Range for ascending spatial failed to satisfy that max (%s) > (%s) initial", range.max(), initial ) );
            }
            // TODO technically will not ascend forever but 1,000,000,000 is large enough to be practically infinite
            long count = 1_000_000_000;
            return grid(
                    initial.doubleValue(),
                    range.max().doubleValue(),
                    initial.doubleValue(),
                    range.max().doubleValue(),
                    count,
                    new Cartesian() );
        case DATE_TIME:
            return dateTime( ascLong( initial.longValue() ) );
        case LOCAL_DATE_TIME:
            return localDatetime( ascLong( initial.longValue() ) );
        case TIME:
            return time( ascLong( initial.intValue() ) );
        case LOCAL_TIME:
            return localTime( ascLong( initial.longValue() ) );
        case DATE:
            return date( ascLong( initial.longValue() ) );
        case DURATION:
            return duration( ascLong( initial.longValue() ) );
        case BYTE_ARR:
            return byteArray( ascByte( initial.byteValue() ), DEFAULT_SIZE );
        case SHORT_ARR:
            return shortArray( ascShort( initial.shortValue() ), DEFAULT_SIZE );
        case INT_ARR:
            return intArray( ascInt( initial.intValue() ), DEFAULT_SIZE );
        case LNG_ARR:
            return longArray( ascLong( initial.longValue() ), DEFAULT_SIZE );
        case FLT_ARR:
            return floatArray( ascFloat( initial.floatValue() ), DEFAULT_SIZE );
        case DBL_ARR:
            return doubleArray( ascDouble( initial.doubleValue() ), DEFAULT_SIZE );
        case STR_SML_ARR:
            return stringArray( intString( ascInt( initial.intValue() ), SMALL_STRING_LENGTH ), DEFAULT_SIZE );
        case STR_BIG_ARR:
            return stringArray( intString( ascInt( initial.intValue() ), BIG_STRING_LENGTH ), DEFAULT_SIZE );
        default:
            throw new RuntimeException( "Unsupported type: " + type );
        }
    }

    /**
     * Generates striding value sequence. Sequence of every thread is guaranteed to never overlap with that of another thread. Sequence of every thread will
     * start at a different offset in the ID sequence, and then wrap back around after exceeding max value -- this is to spread load across the ID range.
     * <p>
     * The above holds only for non-array types. For array types values are random which, presumably, will 'never' result in duplicates.
     *
     * @param threads : must be in range [1..]
     * @param thread  : must be in range [0..threads)
     * @param max     : must be in range [1..]
     * @return Sequence generator factory that generates sequence generators with offset in range [thread..max)
     */
    public static ValueGeneratorFactory nonContendingStridingFor( String type, int threads, int thread, long max )
    {
        // prevents threads from ever accessing same ID
        int stride = threads;
        // scatters access across ID range
        long rangePartitionSize = max / threads;
        // approximate range offset where thread should start. not guaranteed to never overlap with another thread
        long offsetEstimate = rangePartitionSize * thread;
        // will be non-zero if estimate results in multiple threads accessing the same ID
        long offsetError = offsetEstimate % stride;
        // range offset where thread should start. guaranteed to never overlap with another thread
        long correctedOffset = offsetEstimate - offsetError + thread;
        // Note about 'sliding': false ~= minimum thread contention, true ~= maximum thread contention
        boolean sliding = false;

        return stridingFor( type, max, stride, correctedOffset, sliding );
    }

    public static ValueGeneratorFactory stridingFor(
            String type,
            long max,
            int stride,
            long offset,
            boolean sliding )
    {
        switch ( type )
        {
        case INT:
            return stridingInt( stride, toInt( max ), toInt( offset ), sliding );
        case LNG:
            return stridingLong( stride, max, offset, sliding );
        case FLT:
            return stridingFloat( stride, max, offset, sliding );
        case DBL:
            return stridingDouble( stride, max, offset, sliding );
        case STR_INL:
        case STR_SML:
            return intString( stridingInt( stride, toInt( max ), toInt( offset ), sliding ), SMALL_STRING_LENGTH );
        case STR_BIG:
            return intString( stridingInt( stride, toInt( max ), toInt( offset ), sliding ), BIG_STRING_LENGTH );
        case POINT:
            return diagonal( stridingDouble( stride, max, offset, sliding ), new CRS.Cartesian() );
        case DATE_TIME:
            return dateTime( stridingLong( stride, max, offset, sliding ) );
        case LOCAL_DATE_TIME:
            return localDatetime( stridingLong( stride, max, offset, sliding ) );
        case TIME:
            return time( stridingLong( stride, max, offset, sliding ) );
        case LOCAL_TIME:
            return localTime( stridingLong( stride, max, offset, sliding ) );
        case DATE:
            return date( stridingLong( stride, max, offset, sliding ) );
        case DURATION:
            return duration( stridingLong( stride, max, offset, sliding ) );
        case INT_ARR:
            // Note: random rather than striding because striding creates duplicates -- investigate
            // Random should be safe, as duplicates extremely unlikely
            return intArray( randInt( 0, Integer.MAX_VALUE ), DEFAULT_SIZE );
        case LNG_ARR:
            // Note: random rather than striding because striding creates duplicates -- investigate
            // Random should be safe, as duplicates extremely unlikely
            return longArray( randLong( Integer.MAX_VALUE, Long.MAX_VALUE ), DEFAULT_SIZE );
        case FLT_ARR:
            // Note: random rather than striding because striding creates duplicates -- investigate
            // Random should be safe, as duplicates extremely unlikely
            return floatArray( randFloat( 0, Float.MAX_VALUE ), DEFAULT_SIZE );
        case DBL_ARR:
            // Note: random rather than striding because striding creates duplicates -- investigate
            // Random should be safe, as duplicates extremely unlikely
            return doubleArray( randDouble( 0, Double.MAX_VALUE ), DEFAULT_SIZE );
        case STR_SML_ARR:
            // Note: random rather than striding because striding creates duplicates -- investigate
            // Random should be safe, as duplicates extremely unlikely
            return stringArray( randShortUtf8(), DEFAULT_SIZE );
        case STR_BIG_ARR:
            // Note: random rather than striding because striding creates duplicates -- investigate
            // Random should be safe, as duplicates extremely unlikely
            return stringArray( randUtf8( BIG_STRING_LENGTH ), DEFAULT_SIZE );
        default:
            throw new RuntimeException( "Unrecognized type: " + type );
        }
    }

    private static int toInt( long value )
    {
        if ( value > Integer.MAX_VALUE || value < Integer.MIN_VALUE )
        {
            throw new IllegalArgumentException( format( "Value %s was outside int range [%s,%s]",
                                                        value, Integer.MIN_VALUE, Integer.MAX_VALUE ) );
        }
        return (int) value;
    }
}
