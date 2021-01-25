/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class TypeParamValuesTest
{
    @Test
    public void shouldGenerateExpectedNumberOfDistinctValues()
    {
        long seed = System.currentTimeMillis();
        Random rng = new Random( seed );
        List<Integer> valueCounts = Lists.newArrayList( 1000, 10_000, 100_000 );
        for ( Integer valueCount : valueCounts )
        {
            long expectedDistinctCount = Math.round( Math.floor( rng.nextDouble() * valueCount ) );
            List<Long> longs = TypeParamValues.shuffledListOf( TypeParamValues.LNG(), valueCount, (int) expectedDistinctCount );
            long actualDistinctCount = longs.stream().distinct().count();
            assertThat( format( "Seed: %s, Expected: %s, Actual: %s", seed, expectedDistinctCount, actualDistinctCount ),
                        actualDistinctCount,
                        equalTo( expectedDistinctCount ) );
            assertThat( format( "Seed: %s, Expected: %s, Actual: %s", seed, expectedDistinctCount, actualDistinctCount ),
                        longs.size(),
                        equalTo( valueCount ) );
        }
    }
}
