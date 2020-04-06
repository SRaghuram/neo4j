/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Map;

public class AuxiliaryMetrics extends BaseMetrics
{
    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public AuxiliaryMetrics()
    {
        this( null, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1 );
    }

    public AuxiliaryMetrics(
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
        super( unit,
               min,
               max,
               mean,
               sample_size,
               percentile_25,
               percentile_50,
               percentile_75,
               percentile_90,
               percentile_95,
               percentile_99,
               percentile_99_9 );
    }

    public static AuxiliaryMetrics fromMap( Map<String,Object> map )
    {
        return new AuxiliaryMetrics(
                (String) map.get( BaseMetrics.UNIT ),
                (Double) map.get( BaseMetrics.MIN ),
                (Double) map.get( BaseMetrics.MAX ),
                (Double) map.get( BaseMetrics.MEAN ),
                (Long) map.get( BaseMetrics.SAMPLE_SIZE ),
                (Double) map.get( BaseMetrics.PERCENTILE_25 ),
                (Double) map.get( BaseMetrics.PERCENTILE_50 ),
                (Double) map.get( BaseMetrics.PERCENTILE_75 ),
                (Double) map.get( BaseMetrics.PERCENTILE_90 ),
                (Double) map.get( BaseMetrics.PERCENTILE_95 ),
                (Double) map.get( BaseMetrics.PERCENTILE_99 ),
                (Double) map.get( BaseMetrics.PERCENTILE_99_9 ) );
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
}
