/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.model;

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
                (String) map.get( UNIT ),
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
