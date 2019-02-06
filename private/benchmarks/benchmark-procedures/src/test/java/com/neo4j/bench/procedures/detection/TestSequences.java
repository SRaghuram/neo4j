/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.procedures.detection;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.model.Neo4j;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static com.neo4j.bench.client.model.Edition.ENTERPRISE;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

class TestSequences
{
    private static final Neo4j NEO4J = new Neo4j( "commit", "3.1.0", ENTERPRISE, "branch", "owner" );

    static List<Point> literal( TimeUnit unit, double... values )
    {
        return DoubleStream.of( values )
                .mapToObj( d -> new Point(
                        new Double( d ).longValue(),
                        new Double( d ).longValue(),
                        d,
                        unit,
                        NEO4J ) )
                .collect( toList() );
    }

    // TODO use when auto regression detection is implemented for realz
    /*
             *    *   *   *
            / \  / \ / \ /
           *    *   *   *
     */
    static List<Point> uniformRegularNoise( int multiplier )
    {
        return Lists.newArrayList(
                new Point( 1, 1, multiplier * 2.0, MILLISECONDS, NEO4J ),
                new Point( 2, 2, multiplier * 3.0, MILLISECONDS, NEO4J ),
                new Point( 3, 3, multiplier * 2.0, MILLISECONDS, NEO4J ),
                new Point( 4, 4, multiplier * 3.0, MILLISECONDS, NEO4J ),
                new Point( 5, 5, multiplier * 2.0, MILLISECONDS, NEO4J ),
                new Point( 6, 6, multiplier * 3.0, MILLISECONDS, NEO4J ),
                new Point( 7, 7, multiplier * 2.0, MILLISECONDS, NEO4J ),
                new Point( 8, 8, multiplier * 3.0, MILLISECONDS, NEO4J ),
                new Point( 9, 9, multiplier * 2.0, MILLISECONDS, NEO4J ),
                new Point( 10, 10, multiplier * 3.0, MILLISECONDS, NEO4J ),
                new Point( 11, 11, multiplier * 2.0, MILLISECONDS, NEO4J ),
                new Point( 12, 12, multiplier * 3.0, MILLISECONDS, NEO4J ),
                new Point( 13, 13, multiplier * 2.0, MILLISECONDS, NEO4J ),
                new Point( 14, 14, multiplier * 3.0, MILLISECONDS, NEO4J ),
                new Point( 15, 15, multiplier * 2.0, MILLISECONDS, NEO4J ) );
    }

    // TODO use when auto regression detection is implemented for realz
    /*
           *
            \
             *
              \
               *
                \
                 *
                  \
                   *
                    \
                     *
     */
    static List<Point> gradualDegradation( int multiplier )
    {
        return Lists.newArrayList(
                new Point( 1, 1, multiplier * 7.5, MILLISECONDS, NEO4J ),
                new Point( 2, 2, multiplier * 7.0, MILLISECONDS, NEO4J ),
                new Point( 3, 3, multiplier * 6.5, MILLISECONDS, NEO4J ),
                new Point( 4, 4, multiplier * 6.0, MILLISECONDS, NEO4J ),
                new Point( 5, 5, multiplier * 5.5, MILLISECONDS, NEO4J ),
                new Point( 6, 6, multiplier * 5.0, MILLISECONDS, NEO4J ),
                new Point( 7, 7, multiplier * 4.5, MILLISECONDS, NEO4J ),
                new Point( 8, 8, multiplier * 4.0, MILLISECONDS, NEO4J ),
                new Point( 9, 9, multiplier * 3.5, MILLISECONDS, NEO4J ),
                new Point( 10, 10, multiplier * 3.0, MILLISECONDS, NEO4J ),
                new Point( 11, 11, multiplier * 2.5, MILLISECONDS, NEO4J ),
                new Point( 12, 12, multiplier * 2.0, MILLISECONDS, NEO4J ),
                new Point( 13, 13, multiplier * 1.5, MILLISECONDS, NEO4J ),
                new Point( 14, 14, multiplier * 1.0, MILLISECONDS, NEO4J ),
                new Point( 15, 15, multiplier * 0.5, MILLISECONDS, NEO4J ) );
    }

    // TODO use when auto regression detection is implemented for realz
    /*
           *     *     *-*
            \   / \   /   \
             *-*   *-*     *-*-*-*
                                  \
                                   *-*-*
     */
    static List<Point> gradualDegradationWithNoise( int multiplier )
    {
        return Lists.newArrayList(
                new Point( 1, 1, multiplier * 4.0, MILLISECONDS, NEO4J ),
                new Point( 2, 2, multiplier * 3.0, MILLISECONDS, NEO4J ),
                new Point( 3, 3, multiplier * 3.0, MILLISECONDS, NEO4J ),
                new Point( 4, 4, multiplier * 4.0, MILLISECONDS, NEO4J ),
                new Point( 5, 5, multiplier * 3.0, MILLISECONDS, NEO4J ),
                new Point( 6, 6, multiplier * 3.0, MILLISECONDS, NEO4J ),
                new Point( 7, 7, multiplier * 4.0, MILLISECONDS, NEO4J ),
                new Point( 8, 8, multiplier * 4.0, MILLISECONDS, NEO4J ),
                new Point( 9, 9, multiplier * 3.0, MILLISECONDS, NEO4J ),
                new Point( 10, 10, multiplier * 3.0, MILLISECONDS, NEO4J ),
                new Point( 11, 11, multiplier * 3.0, MILLISECONDS, NEO4J ),
                new Point( 12, 12, multiplier * 3.0, MILLISECONDS, NEO4J ),
                new Point( 13, 13, multiplier * 3.0, MILLISECONDS, NEO4J ),
                new Point( 14, 14, multiplier * 2.0, MILLISECONDS, NEO4J ),
                new Point( 15, 15, multiplier * 2.0, MILLISECONDS, NEO4J ) );
    }

    /*
     Series of points where diff between any two consecutive points is value selected from gaussian distribution
     */
    static List<Point> gaussian( double avg, int count )
    {
        Supplier<Double> diffFun = new Supplier<Double>()
        {
            Random random = new Random( 42 );
            double previous;

            @Override
            public Double get()
            {
                double diff = random.nextGaussian() + avg;
                previous = previous + diff;
                return previous;
            }
        };
        return points( diffFun, count );
    }

    /*
     Series of points where diff between any two consecutive points is value selected from uniform distribution
     */
    static List<Point> uniform( double avg, int count )
    {
        Supplier<Double> diffFun = new Supplier<Double>()
        {
            Random random = new Random( 42 );
            double previous;

            @Override
            public Double get()
            {
                double diff = random.nextDouble() + avg - 0.5;
                previous = previous + diff;
                return previous;
            }
        };
        return points( diffFun, count );
    }

    private static List<Point> points( Supplier<Double> diffFun, int count )
    {
        return IntStream.range( 0, count )
                .mapToObj( i -> new Point( i, i, diffFun.get(), MILLISECONDS, NEO4J ) )
                .collect( toList() );
    }
}
