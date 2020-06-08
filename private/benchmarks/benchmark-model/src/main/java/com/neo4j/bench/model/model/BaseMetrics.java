/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;
import java.util.Map;

abstract class BaseMetrics
{
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

    private final String unit;
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
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    protected BaseMetrics()
    {
        this( null, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1 );
    }

    protected BaseMetrics(
            String unit,
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

    public Map<String,Object> toMap()
    {
        Map<String,Object> map = new HashMap<>();
        map.put( UNIT, unit );
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
}
