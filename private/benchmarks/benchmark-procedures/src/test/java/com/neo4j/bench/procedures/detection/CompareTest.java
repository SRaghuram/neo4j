/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.procedures.detection;

import org.junit.Test;

import static com.neo4j.bench.client.model.Benchmark.Mode.LATENCY;
import static com.neo4j.bench.client.model.Benchmark.Mode.THROUGHPUT;
import static com.neo4j.bench.procedures.detection.CompareFunction.CompareCalculator;
import static com.neo4j.bench.procedures.detection.CompareResult.IMPROVEMENT;
import static com.neo4j.bench.procedures.detection.CompareResult.LIKELY_IMPROVEMENT;
import static com.neo4j.bench.procedures.detection.CompareResult.LIKELY_REGRESSION;
import static com.neo4j.bench.procedures.detection.CompareResult.REGRESSION;
import static com.neo4j.bench.procedures.detection.CompareResult.SIMILAR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class CompareTest
{
    @Test
    public void customTest()
    {
        double baseMetrics = 434.56510883826394;
        double baseVariance = 350.51562499976717;
        double compareMetrics = 434.56510883826394;
        double compareVariance = 513.1210937497672;
        CompareCalculator throughput = new CompareCalculator( baseMetrics, baseVariance, compareMetrics, compareVariance, THROUGHPUT.name() );
        System.out.println(throughput);
    }
    @Test
    public void shouldDetectDefiniteImprovement() throws Exception
    {
        int lowerValue = 0;
        int lowerVariance = 1;
        int higherValue = 4;
        int higherVariance = 1;
         /*
             ---
              |
             ---
        ---
         |
        ---
         */

        CompareCalculator throughput = new CompareCalculator( lowerValue, lowerVariance, higherValue, higherVariance, THROUGHPUT.name() );
        /*
         ---
          |
         ---
            ---
             |
            ---
         */

        CompareCalculator latency = new CompareCalculator( higherValue, higherVariance, lowerValue, lowerVariance, LATENCY.name() );

        assertThat( latency.calculate(), equalTo( IMPROVEMENT ) );
        assertThat( throughput.calculate(), equalTo( IMPROVEMENT ) );
    }

    @Test
    public void shouldDetectDefiniteRegression() throws Exception
    {
        int lowerValue = 0;
        int lowerVariance = 1;
        int higherValue = 4;
        int higherVariance = 1;
        /*
         ---
          |
         ---
            ---
             |
            ---
         */

        CompareCalculator throughput = new CompareCalculator( higherValue, higherVariance, lowerValue, lowerVariance, THROUGHPUT.name() );
         /*
             ---
              |
             ---
        ---
         |
        ---
         */

        CompareCalculator latency = new CompareCalculator( lowerValue, lowerVariance, higherValue, higherVariance, LATENCY.name() );

        assertThat( latency.calculate(), equalTo( REGRESSION ) );
        assertThat( throughput.calculate(), equalTo( REGRESSION ) );
    }

    @Test
    public void shouldDetectSimilar() throws Exception
    {
        /*
        ---
         |   ---
         |    |
         |   ---
        ---
         */
        int biggerValue = 3;
        int biggerVariance = 3;
        int smallerValue = 2;
        int smallerVariance = 1;

        CompareCalculator latency = new CompareCalculator( biggerValue, biggerVariance, smallerValue, smallerVariance, LATENCY.name() );

        CompareCalculator throughput = new CompareCalculator( biggerValue, biggerVariance, smallerValue, smallerVariance, THROUGHPUT.name() );

        assertThat( latency.calculate(), equalTo( SIMILAR ) );
        assertThat( throughput.calculate(), equalTo( SIMILAR ) );

        /*
             ---
        ---   |
         |    |
        ---   |
             ---
         */

        latency = new CompareCalculator( smallerValue, smallerVariance, biggerValue, biggerVariance, LATENCY.name() );

        throughput = new CompareCalculator( smallerValue, smallerVariance, biggerValue, biggerVariance, THROUGHPUT.name() );

        assertThat( latency.calculate(), equalTo( SIMILAR ) );
        assertThat( throughput.calculate(), equalTo( SIMILAR ) );

        /*
        ---  ---
         |    |
        ---  ---
         */
        latency = new CompareCalculator( smallerValue, smallerVariance, smallerValue, smallerVariance, LATENCY.name() );

        throughput = new CompareCalculator( smallerValue, smallerVariance, smallerValue, smallerVariance, THROUGHPUT.name() );

        assertThat( latency.calculate(), equalTo( SIMILAR ) );
        assertThat( throughput.calculate(), equalTo( SIMILAR ) );
    }

    @Test
    public void shouldDetectSlightImprovement() throws Exception
    {

          /*
             ---
        ---   |
         |    |
         |   ---
        ---
         */
        int firstValue = 1;
        int firstVariance = 1;
        int laterValue = 2;
        int laterVariance = 1;

        CompareCalculator throughput = new CompareCalculator( firstValue, firstVariance, laterValue, laterVariance, THROUGHPUT.name() );

        assertThat( throughput.calculate(), equalTo( LIKELY_IMPROVEMENT ) );
          /*
        ---  ---
         |    |
         |   ---
        ---
         */
        firstValue = 2;
        firstVariance = 2;
        laterValue = 3;
        laterVariance = 1;

        throughput = new CompareCalculator( firstValue, firstVariance, laterValue, laterVariance, THROUGHPUT.name() );

        assertThat( throughput.calculate(), equalTo( LIKELY_IMPROVEMENT ) );
        /*
        ---  ---
         |    |
        ---   |
             ---
         */
        firstValue = 3;
        firstVariance = 1;
        laterValue = 2;
        laterVariance = 2;

        CompareCalculator latency = new CompareCalculator( firstValue, firstVariance, laterValue, laterVariance, LATENCY.name() );

        assertThat( latency.calculate(), equalTo( LIKELY_IMPROVEMENT ) );
         /*

        ---
         |  ---
         |   |
        ---  |
            ---
         */
        firstValue = 2;
        firstVariance = 1;
        laterValue = 1;
        laterVariance = 1;
        latency = new CompareCalculator( firstValue, firstVariance, laterValue, laterVariance, LATENCY.name() );

        assertThat( latency.calculate(), equalTo( LIKELY_IMPROVEMENT ) );
         /*
        ---
         |  ---
         |   |
        --- ---
         */

        firstValue = 2;
        firstVariance = 2;
        laterValue = 1;
        laterVariance = 1;
        latency = new CompareCalculator( firstValue, firstVariance, laterValue, laterVariance, LATENCY.name() );

        assertThat( latency.calculate(), equalTo( LIKELY_IMPROVEMENT ) );

        /*
             ---
        ---   |
         |    |
        ---  ---
         */

        firstValue = 1;
        firstVariance = 1;
        laterValue = 2;
        laterVariance = 2;

        throughput = new CompareCalculator( firstValue, firstVariance, laterValue, laterVariance, THROUGHPUT.name() );

        assertThat( throughput.calculate(), equalTo( LIKELY_IMPROVEMENT ) );
    }

    @Test
    public void shouldDetectSlightRegression() throws Exception
    {
                  /*
             ---
        ---   |
         |    |
         |   ---
        ---
         */
        int firstValue = 1;
        int firstVariance = 1;
        int laterValue = 2;
        int laterVariance = 1;

        CompareCalculator latency = new CompareCalculator( firstValue, firstVariance, laterValue, laterVariance, LATENCY.name() );

        assertThat( latency.calculate(), equalTo( LIKELY_REGRESSION ) );
          /*
        ---  ---
         |    |
         |   ---
        ---
         */
        firstValue = 2;
        firstVariance = 2;
        laterValue = 3;
        laterVariance = 1;

        latency = new CompareCalculator( firstValue, firstVariance, laterValue, laterVariance, LATENCY.name() );

        assertThat( latency.calculate(), equalTo( LIKELY_REGRESSION ) );
        /*
        ---  ---
         |    |
        ---   |
             ---
         */
        firstValue = 3;
        firstVariance = 1;
        laterValue = 2;
        laterVariance = 2;

        CompareCalculator throughput = new CompareCalculator( firstValue, firstVariance, laterValue, laterVariance, THROUGHPUT.name() );

        assertThat( throughput.calculate(), equalTo( LIKELY_REGRESSION ) );
         /*

        ---
         |  ---
         |   |
        ---  |
            ---
         */
        firstValue = 2;
        firstVariance = 1;
        laterValue = 1;
        laterVariance = 1;
        throughput = new CompareCalculator( firstValue, firstVariance, laterValue, laterVariance, THROUGHPUT.name() );

        assertThat( throughput.calculate(), equalTo( LIKELY_REGRESSION ) );
         /*
        ---
         |  ---
         |   |
        --- ---
         */

        firstValue = 2;
        firstVariance = 2;
        laterValue = 1;
        laterVariance = 1;

        throughput = new CompareCalculator( firstValue, firstVariance, laterValue, laterVariance, THROUGHPUT.name() );

        assertThat( throughput.calculate(), equalTo( LIKELY_REGRESSION ) );

        /*
             ---
        ---   |
         |    |
        ---  ---
         */

        firstValue = 1;
        firstVariance = 1;
        laterValue = 2;
        laterVariance = 2;

        latency = new CompareCalculator( firstValue, firstVariance, laterValue, laterVariance, LATENCY.name() );

        assertThat( latency.calculate(), equalTo( LIKELY_REGRESSION ) );
    }
}
