package com.neo4j.bench.procedures.detection;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.neo4j.bench.client.Units.toAbbreviation;
import static com.neo4j.bench.procedures.detection.Point.BY_DATE;
import static com.neo4j.bench.procedures.detection.Series.saneUnitFor;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Variance
{
    public static Variance calculateFor( Series series )
    {
        switch ( series.size() )
        {
        case 0:
            return defaultVarianceFor( MILLISECONDS );
        case 1:
            return defaultVarianceFor( series.points( BY_DATE ).get( 0 ).unit() );
        default:
            return calculateVarianceFor( series.convertTo( saneUnitFor( series ) ) );
        }
    }

    private static Variance defaultVarianceFor( TimeUnit unit )
    {
        // With one point there is no variance
        double[] percentiles = IntStream.range( 0, 101 ).mapToDouble( i -> 0 ).toArray();
        return new Variance( new double[]{0}, percentiles, unit, 0 );
    }

    private static Variance calculateVarianceFor( Series series )
    {
        List<Point> points = series.points( BY_DATE );
        double[] diffs = new double[points.size() - 1];
        double[] values = points.stream().mapToDouble( Point::value ).toArray();
        IntStream.range( 1, values.length ).forEach( i -> diffs[i - 1] = values[i] - values[i - 1] );
        double mean = Arrays.stream( diffs ).sum() / diffs.length;

        double[] orderedDiffs = Arrays.copyOf( diffs, diffs.length );
        Arrays.sort( orderedDiffs );
        double[] percentiles = new double[101];
        IntStream.range( 0, 101 ).forEach( i ->
                                           {
                                               int index = Math.min( (int) Math.round( (orderedDiffs.length / 100D) * i ), orderedDiffs.length - 1 );
                                               percentiles[i] = orderedDiffs[index];
                                           } );

        return new Variance( diffs, percentiles, points.get( 0 ).unit(), mean );
    }

    private final double[] diffs;
    private final double[] percentiles;
    private final TimeUnit unit;
    private final double mean;

    private Variance( double[] diffs, double[] percentiles, TimeUnit unit, double mean )
    {
        this.diffs = diffs;
        this.percentiles = percentiles;
        this.unit = unit;
        this.mean = mean;
    }

    TimeUnit unit()
    {
        return unit;
    }

    double[] diffs()
    {
        return diffs;
    }

    double diffAtPercentile( int quantile )
    {
        if ( 0 > quantile || quantile > 100 )
        {
            throw new RuntimeException( "Quantile must be in range [0,100], but was: " + quantile );
        }
        return percentiles[quantile];
    }

    double mean()
    {
        return mean;
    }

    @Override
    public String toString()
    {
        return format( "Variance(" +
                       "unit : %s - " +
                       "min : %s - " +
                       "median : %s - " +
                       "mean : %s - " +
                       "75th : %s - " +
                       "max : %s - " +
                       ")",
                       toAbbreviation( unit() ),
                       diffAtPercentile( 0 ),
                       diffAtPercentile( 50 ),
                       mean(),
                       diffAtPercentile( 75 ),
                       diffAtPercentile( 100 ) );
    }
}
