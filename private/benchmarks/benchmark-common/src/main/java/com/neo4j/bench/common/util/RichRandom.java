/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.util;

import java.util.SplittableRandom;

public class RichRandom
{
    private final SplittableRandom rng;

    public RichRandom( int seed )
    {
        this( new SplittableRandom( seed ) );
    }

    public RichRandom( SplittableRandom rng )
    {
        this.rng = rng;
    }

    public SplittableRandom innerRng()
    {
        return rng;
    }

    public int nextInt( int lowerBound, int upperBound )
    {
        return rng.nextInt( lowerBound, upperBound );
    }

    public double nextDouble( double lowerBound, double upperBound )
    {
        return rng.nextDouble( lowerBound, upperBound );
    }

    public int nextInt( int bound )
    {
        return rng.nextInt( bound );
    }

    public double nextDouble()
    {
        return rng.nextDouble();
    }

    // NOTE: Gaussian algorithm based on java.util.Random, SplittableRandom does not support Gaussian
    public static class GaussianState
    {
        private double nextNextGaussian;
        private boolean haveNextNextGaussian;
    }

    private final GaussianState state = new GaussianState();

    public double nextGaussian()
    {
        return nextGaussian( state, rng );
    }

    public static double nextGaussian( GaussianState state, SplittableRandom rng )
    {
        // See Knuth, ACP, Section 3.4.1 Algorithm C.
        if ( state.haveNextNextGaussian )
        {
            state.haveNextNextGaussian = false;
            return state.nextNextGaussian;
        }
        else
        {
            double v1, v2, s;
            do
            {
                v1 = 2 * rng.nextDouble() - 1; // between -1 and 1
                v2 = 2 * rng.nextDouble() - 1; // between -1 and 1
                s = v1 * v1 + v2 * v2;
            }
            while ( s >= 1 || s == 0 );
            double multiplier = StrictMath.sqrt( -2 * StrictMath.log( s ) / s );
            state.nextNextGaussian = v2 * multiplier;
            state.haveNextNextGaussian = true;
            return v1 * multiplier;
        }
    }
}
