/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import com.neo4j.bench.micro.data.ConstantGenerator.ConstantGeneratorFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.stream.Stream;

public abstract class DiscreteGenerator
{
    public static ValueGeneratorFactory discrete( Bucket... buckets )
    {
        return new DiscreteBucketGeneratorFactory<>( buckets );
    }

    public static class Bucket
    {
        private double ratio;
        private ConstantGeneratorFactory value;

        private Bucket()
        {
        }

        public Bucket( double ratio, ConstantGeneratorFactory value )
        {
            this.ratio = ratio;
            this.value = value;
        }

        public double ratio()
        {
            return ratio;
        }

        public Object value()
        {
            return value.create().next( null );
        }

        @Override
        public String toString()
        {
            return "(" + "ratio=" + ratio + ", value=" + value() + ')';
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
            Bucket bucket = (Bucket) o;
            return Double.compare( bucket.ratio, ratio ) == 0 && value.equals( bucket.value );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( ratio, value );
        }
    }

    private static class DiscreteBucketGeneratorFactory<VALUE_TYPE> implements ValueGeneratorFactory<VALUE_TYPE>
    {
        private Bucket[] buckets;

        private DiscreteBucketGeneratorFactory()
        {
        }

        private DiscreteBucketGeneratorFactory( Bucket[] buckets )
        {
            this.buckets = buckets;
        }

        @Override
        public ValueGeneratorFun<VALUE_TYPE> create()
        {
            return new DiscreteBucketGenerator( buckets );
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
            DiscreteBucketGeneratorFactory<?> that = (DiscreteBucketGeneratorFactory<?>) o;
            return Arrays.equals( buckets, that.buckets );
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode( buckets );
        }
    }

    /**
     * Generate sequence of numbers, selecting randomly only from those provided.
     * Additionally, each number is selected with the probability specified.
     */
    private static class DiscreteBucketGenerator implements ValueGeneratorFun
    {
        private final double ratiosSum;
        private final double[] cumulativeRatios;
        private final List values;

        private DiscreteBucketGenerator( Bucket... buckets )
        {
            if ( 0 == buckets.length )
            {
                throw new RuntimeException( "One or more buckets must be provided" );
            }
            this.ratiosSum = Stream.of( buckets ).mapToDouble( Bucket::ratio ).sum();
            this.cumulativeRatios = new double[buckets.length];
            this.cumulativeRatios[0] = buckets[0].ratio();
            this.values = new ArrayList<>();
            this.values.add( 0, buckets[0].value() );
            for ( int i = 1; i < buckets.length; i++ )
            {
                this.cumulativeRatios[i] = buckets[i].ratio() + this.cumulativeRatios[i - 1];
                this.values.add( i, buckets[i].value() );
            }
        }

        @Override
        public boolean wrapped()
        {
            return false;
        }

        @Override
        public Object next( SplittableRandom rng )
        {
            final double randomValue = rng.nextDouble() * ratiosSum;
            for ( int i = 0; i < cumulativeRatios.length; i++ )
            {
                if ( randomValue < cumulativeRatios[i] )
                {
                    return values.get( i );
                }
            }
            throw new RuntimeException( "No matching value found" );
        }
    }
}
