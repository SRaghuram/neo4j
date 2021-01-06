/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.neo4j.bench.model.util.UnitConverter;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.neo4j.bench.model.util.UnitConverter.toTimeUnit;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Metrics
{
    public abstract static class MetricsUnit
    {
        public static boolean isLatency( MetricsUnit metricsUnit )
        {
            return metricsUnit instanceof TimeBasedUnit;
        }

        public static MetricsUnit latency( TimeUnit unit )
        {
            return new TimeBasedUnit( unit );
        }

        public static MetricsUnit accuracy()
        {
            return new AccuracyUnit();
        }

        public static MetricsUnit rows()
        {
            return new RowsUnit();
        }

        public static MetricsUnit parse( String value )
        {
            List<Function<String,Optional<MetricsUnit>>> parsers = Lists.newArrayList( AccuracyUnit::tryParse, RowsUnit::tryParse, TimeBasedUnit::tryParse );
            return parsers.stream()
                          .map( parser -> parser.apply( value ) )
                          .filter( Optional::isPresent )
                          .map( Optional::get )
                          .findFirst()
                          .orElseThrow( () -> new IllegalArgumentException( format( "Unable to parse unit: '%s'", value ) ) );
        }

        public abstract String value();

        public abstract boolean isCompatibleMode( Benchmark.Mode mode );

        @Override
        public boolean equals( Object o )
        {
            return EqualsBuilder.reflectionEquals( o, this );
        }

        @Override
        public int hashCode()
        {
            return HashCodeBuilder.reflectionHashCode( this );
        }

        @Override
        public String toString()
        {
            return ToStringBuilder.reflectionToString( this, ToStringStyle.SHORT_PREFIX_STYLE );
        }
    }

    private abstract static class GenericUnit extends MetricsUnit
    {
        private final String value;

        @JsonCreator
        private GenericUnit( @JsonProperty( "value" ) String value )
        {
            this.value = value;
        }

        @Override
        public String value()
        {
            return value;
        }
    }

    private static class AccuracyUnit extends GenericUnit
    {
        private static final String ACCURACY = "accuracy";

        private static Optional<MetricsUnit> tryParse( String value )
        {
            return value.equalsIgnoreCase( ACCURACY )
                   ? Optional.of( new AccuracyUnit() )
                   : Optional.empty();
        }

        @JsonCreator
        private AccuracyUnit()
        {
            super( ACCURACY );
        }

        @Override
        public boolean isCompatibleMode( Benchmark.Mode mode )
        {
            return Benchmark.Mode.ACCURACY.equals( mode );
        }
    }

    private static class RowsUnit extends GenericUnit
    {
        private static String ROWS = "rows";

        private static Optional<MetricsUnit> tryParse( String value )
        {
            return value.equalsIgnoreCase( ROWS )
                   ? Optional.of( new RowsUnit() )
                   : Optional.empty();
        }

        @JsonCreator
        private RowsUnit()
        {
            super( ROWS );
        }

        @Override
        public boolean isCompatibleMode( Benchmark.Mode mode )
        {
            return true;
        }
    }

    private static class TimeBasedUnit extends MetricsUnit
    {
        private static Optional<MetricsUnit> tryParse( String value )
        {
            try
            {
                return Optional.of( new TimeBasedUnit( toTimeUnit( value ) ) );
            }
            catch ( Exception e )
            {
                return Optional.empty();
            }
        }

        private final TimeUnit unit;

        @JsonCreator
        private TimeBasedUnit( @JsonProperty( "unit" ) TimeUnit unit )
        {
            this.unit = unit;
        }

        @Override
        public String value()
        {
            return UnitConverter.toAbbreviation( unit );
        }

        @Override
        public boolean isCompatibleMode( Benchmark.Mode mode )
        {
            return !Benchmark.Mode.ACCURACY.equals( mode );
        }
    }

    public static final String UNIT = "unit";
    public static final String MIN = "min";
    public static final String MAX = "max";
    public static final String MEAN = "mean";
    public static final String SAMPLE_SIZE = "sample_size";
    public static final String PERCENTILE_25 = "perc_25";
    public static final String PERCENTILE_50 = "perc_50";
    public static final String PERCENTILE_75 = "perc_75";
    public static final String PERCENTILE_90 = "perc_90";
    public static final String PERCENTILE_95 = "perc_95";
    public static final String PERCENTILE_99 = "perc_99";
    public static final String PERCENTILE_99_9 = "perc_99_9";

    private final MetricsUnit unit;
    private final double min;
    private final double max;
    private final double mean;
    private final long sample_size;
    private final double percentile_25;
    private final double percentile_50;
    private final double percentile_75;
    private final double percentile_90;
    private final double percentile_95;
    private final double percentile_99;
    private final double percentile_99_9;

    /**
     * WARNING: Never call this explicitly. No-params constructor is only used for JSON (de)serialization.
     */
    protected Metrics()
    {
        this( MetricsUnit.latency( MILLISECONDS ), -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1 );
    }

    public Metrics(
            MetricsUnit unit,
            double min,
            double max,
            double mean,
            long sample_size,
            double percentile_25,
            double percentile_50,
            double percentile_75,
            double percentile_90,
            double percentile_95,
            double percentile_99,
            double percentile_99_9 )
    {
        this.unit = unit;
        this.min = min;
        this.max = max;
        this.mean = mean;
        this.sample_size = sample_size;
        this.percentile_25 = percentile_25;
        this.percentile_50 = percentile_50;
        this.percentile_75 = percentile_75;
        this.percentile_90 = percentile_90;
        this.percentile_95 = percentile_95;
        this.percentile_99 = percentile_99;
        this.percentile_99_9 = percentile_99_9;
    }

    public MetricsUnit unit()
    {
        return unit;
    }

    public Map<String,Object> toMap()
    {
        Map<String,Object> map = new HashMap<>();
        map.put( UNIT, unit.value() );
        map.put( MIN, min );
        map.put( MAX, max );
        map.put( MEAN, mean );
        map.put( SAMPLE_SIZE, sample_size );
        map.put( PERCENTILE_25, percentile_25 );
        map.put( PERCENTILE_50, percentile_50 );
        map.put( PERCENTILE_75, percentile_75 );
        map.put( PERCENTILE_90, percentile_90 );
        map.put( PERCENTILE_95, percentile_95 );
        map.put( PERCENTILE_99, percentile_99 );
        map.put( PERCENTILE_99_9, percentile_99_9 );
        return map;
    }

    @Override
    public boolean equals( Object o )
    {
        return EqualsBuilder.reflectionEquals( this, o );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }

    @Override
    public String toString()
    {
        return "(" + mean + " , " + unit + ")";
    }

    public static Metrics fromMap( Map<String,Object> map )
    {
        return new Metrics(
                MetricsUnit.parse( (String) map.get( UNIT ) ),
                (Double) map.get( MIN ),
                (Double) map.get( MAX ),
                (Double) map.get( MEAN ),
                (Long) map.get( SAMPLE_SIZE ),
                (Double) map.get( PERCENTILE_25 ),
                (Double) map.get( PERCENTILE_50 ),
                (Double) map.get( PERCENTILE_75 ),
                (Double) map.get( PERCENTILE_90 ),
                (Double) map.get( PERCENTILE_95 ),
                (Double) map.get( PERCENTILE_99 ),
                (Double) map.get( PERCENTILE_99_9 ) );
    }
}
