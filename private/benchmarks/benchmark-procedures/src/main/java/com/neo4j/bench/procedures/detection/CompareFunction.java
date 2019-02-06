/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.procedures.detection;

import com.neo4j.bench.client.model.Benchmark.Mode;

import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import static com.neo4j.bench.procedures.detection.CompareResult.IMPROVEMENT;
import static com.neo4j.bench.procedures.detection.CompareResult.LIKELY_IMPROVEMENT;
import static com.neo4j.bench.procedures.detection.CompareResult.LIKELY_REGRESSION;
import static com.neo4j.bench.procedures.detection.CompareResult.REGRESSION;
import static com.neo4j.bench.procedures.detection.CompareResult.SIMILAR;

import static java.lang.String.format;

enum CompareResult
{
    IMPROVEMENT( 2 ),
    LIKELY_IMPROVEMENT( 1 ),
    SIMILAR( 0 ),
    LIKELY_REGRESSION( -1 ),
    REGRESSION( -2 );
    final long result;

    CompareResult( long result )
    {
        this.result = result;
    }
}

public class CompareFunction
{
    @UserFunction( name = "bench.compare" )
    public long compare(
            @Name( "baseValue" ) double baseValue,
            @Name( "baseVariance" ) double baseVariance,
            @Name( "compareValue" ) double compareValue,
            @Name( "compareVariance" ) double compareVariance,
            @Name( "mode" ) String mode )
    {
        return new CompareCalculator( baseValue,
                                      baseVariance,
                                      compareValue,
                                      compareVariance,
                                      mode ).calculate().result;
    }

    public static class CompareCalculator
    {
        private static double baseValue;
        private static double baseVariance;
        private static double compareValue;
        private static double compareVariance;
        private static Mode mode;

        public CompareCalculator( double baseValue, double baseVariance, double compareValue, double compareVariance, String mode )
        {
            this.baseValue = baseValue;
            this.baseVariance = baseVariance;
            this.compareValue = compareValue;
            this.compareVariance = compareVariance;
            this.mode = Mode.valueOf( mode );
        }

        public CompareResult calculate()
        {
            double worstBase = worst( baseValue, baseVariance, mode );
            double bestBase = best( baseValue, baseVariance, mode );
            double worstCompare = worst( compareValue, compareVariance, mode );
            double bestCompare = best( compareValue, compareVariance, mode );

            // clear improvement : worst compare value is still better than best base value
            if ( compare( bestBase, worstCompare, mode ) == 1 )
            {
                return IMPROVEMENT;
            }

            // clear regression : worst base value is still better than best compare value
            if ( compare( worstBase, bestCompare, mode ) == -1 )
            {
                return REGRESSION;
            }

            int compareBest = compare( bestBase, bestCompare, mode );
            int compareWorst = compare( worstBase, worstCompare, mode );

            // equal : base and compare have the same worst and best values, they are equal
            if ( compareBest + compareWorst == 0 )
            {
                return SIMILAR;
            }

            // likely improvement : best base value is worse/equal to best compare value AND worst base value is worse/equal to worst compare value
            if ( compareBest >= 0 && compareWorst >= 0 )
            {
                return LIKELY_IMPROVEMENT;
            }

            // likely regression : best base value is better/equal than best compare value AND worst base value is better/equal than worst compare value
            if ( compareBest <= 0 && compareWorst <= 0 )
            {
                return LIKELY_REGRESSION;
            }

            String values = format( "compareBest=%s, compareWorst=%s, worstBase=%s, bestBase=%s, worstCompare=%s, bestCompare=%s",
                                    compareBest, compareWorst, worstBase, bestBase, worstCompare, bestCompare );

            throw new RuntimeException( "Comparison fell through the chairs: " + values );
        }

        private static int compare( double v1, double v2, Mode mode )
        {
            switch ( mode )
            {
            case LATENCY:
            case SINGLE_SHOT:
                return Double.compare( v1, v2 );
            case THROUGHPUT:
                return Double.compare( v2, v1 );
            default:
                throw new RuntimeException( "Unrecognized mode: " + mode );
            }
        }

        private static double worst( double value, double variance, Mode mode )
        {
            switch ( mode )
            {
            case LATENCY:
            case SINGLE_SHOT:
                return value + variance;
            case THROUGHPUT:
                return value - variance;
            default:
                throw new RuntimeException( "Unrecognized mode: " + mode );
            }
        }

        private static double best( double value, double variance, Mode mode )
        {
            switch ( mode )
            {
            case LATENCY:
            case SINGLE_SHOT:
                return value - variance;
            case THROUGHPUT:
                return value + variance;
            default:
                throw new RuntimeException( "Unrecognized mode: " + mode );
            }
        }
    }
}
