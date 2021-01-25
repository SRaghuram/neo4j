/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.measurement;

import java.util.Arrays;
import java.util.function.DoubleFunction;
import java.util.stream.DoubleStream;

public class AggregateMeasurement
{
    public static AggregateMeasurement calculateFrom( double[] measurements )
    {
        if ( measurements.length == 0 )
        {
            throw new RuntimeException( "No measurements provided" );
        }
        return new AggregateMeasurement( measurements );
    }

    private final double[] measurements;
    private final double mean;

    private AggregateMeasurement( double[] measurements )
    {
        this.measurements = measurements;
        Arrays.sort( measurements );
        this.mean = DoubleStream.of( measurements ).average().orElse( 0 );
    }

    public double min()
    {
        return percentile( 0D );
    }

    public double median()
    {
        return percentile( 0.5D );
    }

    public double max()
    {
        return percentile( 1.0D );
    }

    public double mean()
    {
        return mean;
    }

    public long count()
    {
        return measurements.length;
    }

    public double percentile( double percentile )
    {
        assertPercentileInRange( percentile );
        int index = (int) (percentile * measurements.length);
        if ( measurements.length == index )
        {
            index = index - 1;
        }
        return measurements[index];
    }

    @Override
    public String toString()
    {
        return "Measurements\n" +
               "  * mean      :  " + mean() + "\n" +
               "  * min       :  " + min() + "\n" +
               "  * perc(0)   :  " + percentile( 0.0D ) + "\n" +
               "  * perc(10)  :  " + percentile( 0.1D ) + "\n" +
               "  * perc(20)  :  " + percentile( 0.2D ) + "\n" +
               "  * perc(30)  :  " + percentile( 0.3D ) + "\n" +
               "  * perc(40)  :  " + percentile( 0.4D ) + "\n" +
               "  * perc(50)  :  " + percentile( 0.5D ) + "\n" +
               "  * perc(60)  :  " + percentile( 0.6D ) + "\n" +
               "  * perc(70)  :  " + percentile( 0.7D ) + "\n" +
               "  * perc(80)  :  " + percentile( 0.8D ) + "\n" +
               "  * perc(90)  :  " + percentile( 0.9D ) + "\n" +
               "  * perc(100) :  " + percentile( 1.0D ) + "\n" +
               "  * max       :  " + max();
    }

    private void assertPercentileInRange( double percentile )
    {
        if ( 0D > percentile || percentile > 1D )
        {
            throw new RuntimeException( "Percentile must be in range [0..100]" );
        }
    }

    public AggregateMeasurement convert( DoubleFunction<Double> convertFun )
    {
        return new AggregateMeasurement(
                DoubleStream.of( measurements )
                            .map( convertFun::apply )
                            .toArray() );
    }
}
