package com.neo4j.bench.client.model;

import com.neo4j.bench.client.Units;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class Metrics
{
    public static final String UNIT = "unit";
    public static final String MIN = "min";
    public static final String MAX = "max";
    public static final String MEAN = "mean";
    public static final String ERROR = "error";
    public static final String ERROR_CONFIDENCE = "error_confidence";
    public static final String SAMPLE_SIZE = "sample_size";
    public static final String PERCENTILE_25 = "perc_25";
    public static final String PERCENTILE_50 = "perc_50";
    public static final String PERCENTILE_75 = "perc_75";
    public static final String PERCENTILE_90 = "perc_90";
    public static final String PERCENTILE_95 = "perc_95";
    public static final String PERCENTILE_99 = "perc_99";
    public static final String PERCENTILE_99_9 = "perc_99_9";

    private final TimeUnit unit;
    private final double min;
    private final double max;
    private final double mean;
    private final double error;
    private final double error_confidence;
    private final long sample_size;
    private final double percentile_25;
    private final double percentile_50;
    private final double percentile_75;
    private final double percentile_90;
    private final double percentile_95;
    private final double percentile_99;
    private final double percentile_99_9;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public Metrics()
    {
        this( TimeUnit.SECONDS, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1 );
    }

    public Metrics(
            TimeUnit unit,
            double min,
            double max,
            double mean,
            double error,
            double error_confidence,
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
        this.error = error;
        this.error_confidence = error_confidence;
        this.sample_size = sample_size;
        this.percentile_25 = percentile_25;
        this.percentile_50 = percentile_50;
        this.percentile_75 = percentile_75;
        this.percentile_90 = percentile_90;
        this.percentile_95 = percentile_95;
        this.percentile_99 = percentile_99;
        this.percentile_99_9 = percentile_99_9;
    }

    public static Metrics fromMap( Map<String,Object> map )
    {
        return new Metrics(
                Units.toTimeUnit( (String) map.get( UNIT ) ),
                (Double) map.get( MIN ),
                (Double) map.get( MAX ),
                (Double) map.get( MEAN ),
                (Double) map.get( ERROR ),
                (Double) map.get( ERROR_CONFIDENCE ),
                (Long) map.get( SAMPLE_SIZE ),
                (Double) map.get( PERCENTILE_25 ),
                (Double) map.get( PERCENTILE_50 ),
                (Double) map.get( PERCENTILE_75 ),
                (Double) map.get( PERCENTILE_90 ),
                (Double) map.get( PERCENTILE_95 ),
                (Double) map.get( PERCENTILE_99 ),
                (Double) map.get( PERCENTILE_99_9 ) );
    }

    public Map<String,Object> toMap()
    {
        Map<String,Object> map = new HashMap<>();
        map.put( UNIT, Units.toAbbreviation( unit ) );
        map.put( MIN, min );
        map.put( MAX, max );
        map.put( MEAN, mean );
        map.put( ERROR, error );
        map.put( ERROR_CONFIDENCE, error_confidence );
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
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        Metrics metrics = (Metrics) o;
        return Double.compare( metrics.min, min ) == 0 &&
               Double.compare( metrics.max, max ) == 0 &&
               Double.compare( metrics.mean, mean ) == 0 &&
               Double.compare( metrics.error, error ) == 0 &&
               Double.compare( metrics.error_confidence, error_confidence ) == 0 &&
               sample_size == metrics.sample_size &&
               Double.compare( metrics.percentile_25, percentile_25 ) == 0 &&
               Double.compare( metrics.percentile_50, percentile_50 ) == 0 &&
               Double.compare( metrics.percentile_75, percentile_75 ) == 0 &&
               Double.compare( metrics.percentile_90, percentile_90 ) == 0 &&
               Double.compare( metrics.percentile_95, percentile_95 ) == 0 &&
               Double.compare( metrics.percentile_99, percentile_99 ) == 0 &&
               Double.compare( metrics.percentile_99_9, percentile_99_9 ) == 0 &&
               unit == metrics.unit;
    }

    @Override
    public int hashCode()
    {
        return Objects
                .hash( unit, min, max, mean, error, error_confidence, sample_size, percentile_25,
                        percentile_50,
                        percentile_75, percentile_90, percentile_95, percentile_99, percentile_99_9 );
    }

    @Override
    public String toString()
    {
        return "(" + mean + " , " + unit + ")";
    }

}
