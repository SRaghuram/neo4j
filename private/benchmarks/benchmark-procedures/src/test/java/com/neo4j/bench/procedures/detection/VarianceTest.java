package com.neo4j.bench.procedures.detection;

import com.neo4j.bench.client.Units;
import org.junit.Test;

import java.util.stream.IntStream;

import static com.neo4j.bench.client.model.Benchmark.Mode.LATENCY;
import static com.neo4j.bench.procedures.detection.TestSequences.gaussian;
import static com.neo4j.bench.procedures.detection.TestSequences.literal;
import static com.neo4j.bench.procedures.detection.TestSequences.uniform;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class VarianceTest
{
    private static final String NEO4J_SERIES = "3.1.0";

    @Test
    public void shouldWorkForSinglePoint() throws Exception
    {
        Series seriesSingleton = new Series( NEO4J_SERIES, literal( MILLISECONDS, 10 ), LATENCY );
        Variance varianceSingleton = Variance.calculateFor( seriesSingleton );

        double conversionFactorSingleton = Units.conversionFactor( varianceSingleton.unit(), MILLISECONDS, LATENCY );

        assertThat( varianceSingleton.mean() * conversionFactorSingleton, equalTo( 0.00D ) );

        IntStream.range( 0, 100 ).forEach( i -> assertThat(
                varianceSingleton.diffAtPercentile( i ),
                lessThanOrEqualTo( varianceSingleton.diffAtPercentile( i + 1 ) ) ) );
    }
    // TODO write tests on hand crafted distributions

    @Test
    public void shouldComputeVarianceForGaussian() throws Exception
    {
        Series seriesNeg1 = new Series( NEO4J_SERIES, gaussian( -1, 100_000 ), LATENCY );
        Series series1 = new Series( NEO4J_SERIES, gaussian( 1, 100_000 ), LATENCY );
        Series seriesNeg10 = new Series( NEO4J_SERIES, gaussian( -10, 100_000 ), LATENCY );
        Series series10 = new Series( NEO4J_SERIES, gaussian( 10, 100_000 ), LATENCY );
        Variance varianceNeg1 = Variance.calculateFor( seriesNeg1 );
        Variance variance1 = Variance.calculateFor( series1 );
        Variance varianceNeg10 = Variance.calculateFor( seriesNeg10 );
        Variance variance10 = Variance.calculateFor( series10 );

        double conversionFactorNeg1 = Units.conversionFactor( varianceNeg1.unit(), MILLISECONDS, LATENCY );
        double conversionFactor1 = Units.conversionFactor( variance1.unit(), MILLISECONDS, LATENCY );
        double conversionFactorNeg10 = Units.conversionFactor( varianceNeg10.unit(), MILLISECONDS, LATENCY );
        double conversionFactor10 = Units.conversionFactor( variance10.unit(), MILLISECONDS, LATENCY );

        assertThat( varianceNeg1.mean() * conversionFactorNeg1,
                allOf( greaterThanOrEqualTo( -1.001D ), lessThanOrEqualTo( -0.999D ) ) );
        assertThat( variance1.mean() * conversionFactor1,
                allOf( greaterThanOrEqualTo( 0.999D ), lessThanOrEqualTo( 1.001D ) ) );
        assertThat( varianceNeg10.mean() * conversionFactorNeg10,
                allOf( greaterThanOrEqualTo( -10.001D ), lessThanOrEqualTo( -9.999D ) ) );
        assertThat( variance10.mean() * conversionFactor10,
                allOf( greaterThanOrEqualTo( 9.999D ), lessThanOrEqualTo( 10.001D ) ) );

        IntStream.range( 0, 100 ).forEach( i ->
                assertThat( variance1.diffAtPercentile( i ),
                        lessThanOrEqualTo( variance1.diffAtPercentile( i + 1 ) ) ) );
        IntStream.range( 0, 100 ).forEach( i ->
                assertThat( varianceNeg1.diffAtPercentile( i ),
                        lessThanOrEqualTo( varianceNeg1.diffAtPercentile( i + 1 ) ) ) );
        IntStream.range( 0, 100 ).forEach( i ->
                assertThat( varianceNeg10.diffAtPercentile( i ),
                        lessThanOrEqualTo( varianceNeg10.diffAtPercentile( i + 1 ) ) ) );
        IntStream.range( 0, 100 ).forEach( i ->
                assertThat( variance10.diffAtPercentile( i ),
                        lessThanOrEqualTo( variance10.diffAtPercentile( i + 1 ) ) ) );
    }

    @Test
    public void shouldComputeVarianceForUniform() throws Exception
    {
        Series seriesNeg1 = new Series( NEO4J_SERIES, uniform( -1, 100_000 ), LATENCY );
        Series series1 = new Series( NEO4J_SERIES, uniform( 1, 100_000 ), LATENCY );
        Series seriesNeg10 = new Series( NEO4J_SERIES, uniform( -10, 100_000 ), LATENCY );
        Series series10 = new Series( NEO4J_SERIES, uniform( 10, 100_000 ), LATENCY );
        Variance varianceNeg1 = Variance.calculateFor( seriesNeg1 );
        Variance variance1 = Variance.calculateFor( series1 );
        Variance varianceNeg10 = Variance.calculateFor( seriesNeg10 );
        Variance variance10 = Variance.calculateFor( series10 );

        double conversionFactorNeg1 = Units.conversionFactor( varianceNeg1.unit(), MILLISECONDS, LATENCY );
        double conversionFactor1 = Units.conversionFactor( variance1.unit(), MILLISECONDS, LATENCY );
        double conversionFactorNeg10 = Units.conversionFactor( varianceNeg10.unit(), MILLISECONDS, LATENCY );
        double conversionFactor10 = Units.conversionFactor( variance10.unit(), MILLISECONDS, LATENCY );

        assertThat( varianceNeg1.mean() * conversionFactorNeg1,
                allOf( greaterThanOrEqualTo( -1.001D ), lessThanOrEqualTo( -0.999D ) ) );
        assertThat( variance1.mean() * conversionFactor1,
                allOf( greaterThanOrEqualTo( 0.999D ), lessThanOrEqualTo( 1.001D ) ) );
        assertThat( varianceNeg10.mean() * conversionFactorNeg10,
                allOf( greaterThanOrEqualTo( -10.001D ), lessThanOrEqualTo( -9.999D ) ) );
        assertThat( variance10.mean() * conversionFactor10,
                allOf( greaterThanOrEqualTo( 9.999D ), lessThanOrEqualTo( 10.001D ) ) );

        IntStream.range( 0, 100 ).forEach( i ->
                assertThat( variance1.diffAtPercentile( i ),
                        lessThanOrEqualTo( variance1.diffAtPercentile( i + 1 ) ) ) );
        IntStream.range( 0, 100 ).forEach( i ->
                assertThat( varianceNeg1.diffAtPercentile( i ),
                        lessThanOrEqualTo( varianceNeg1.diffAtPercentile( i + 1 ) ) ) );
        IntStream.range( 0, 100 ).forEach( i ->
                assertThat( varianceNeg10.diffAtPercentile( i ),
                        lessThanOrEqualTo( varianceNeg10.diffAtPercentile( i + 1 ) ) ) );
        IntStream.range( 0, 100 ).forEach( i ->
                assertThat( variance10.diffAtPercentile( i ),
                        lessThanOrEqualTo( variance10.diffAtPercentile( i + 1 ) ) ) );
    }

    @Test( expected = Exception.class )
    public void shouldThrowExceptionWhenTryingToAccessNegativePercentile() throws Exception
    {
        Series series = new Series( NEO4J_SERIES, uniform( -1, 2 ), LATENCY );
        Variance variance = Variance.calculateFor( series );
        variance.diffAtPercentile( -1 );
    }

    @Test( expected = Exception.class )
    public void shouldThrowExceptionWhenTryingToAccessPercentileGreaterThan100() throws Exception
    {
        Series series = new Series( NEO4J_SERIES, uniform( -1, 2 ), LATENCY );
        Variance variance = Variance.calculateFor( series );
        variance.diffAtPercentile( 101 );
    }
}
