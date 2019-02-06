/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.procedures.detection;

import com.neo4j.bench.client.model.Benchmark.Mode;

import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.neo4j.bench.procedures.detection.Anomaly.AnomalyType.IMPROVEMENT;
import static com.neo4j.bench.procedures.detection.Anomaly.AnomalyType.REGRESSION;
import static com.neo4j.bench.procedures.detection.Point.BY_DATE;
import static com.neo4j.bench.procedures.detection.Point.convertTo;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class Anomalies
{
    // TODO revise, to test if this is necessary
    // fudge-factor multiplier that is applied to variance to prevent reporting of regressions that are just noise
    private static final double VARIANCE_BUFFER = 1.50;

    static Anomalies calculateFor( Series series, Variance variance, double percentage )
    {
        TimeUnit unit = Series.saneUnitFor( series );
        series = series.convertTo( unit );

        List<Point> points = series.points( BY_DATE );
        Map<Point,Anomaly> anomalies = new HashMap<>();
        // TODO uncomment or delete. does this policy add any value?
        // compare against immediately previous raw value
//        usingRawPolicy( points, variance, series.mode(), percentage ).anomalies().stream()
//                .filter( anomaly -> !anomalies.containsKey( anomaly.point() ) )
//                .forEach( anomaly -> anomalies.put( anomaly.point(), anomaly ) );
        // compare against latest point that an anomaly was detected for
        usingGlobalPolicy( points, variance, series.mode(), percentage ).anomalies().stream()
                                                                        .filter( anomaly -> !anomalies.containsKey( anomaly.point() ) )
                                                                        .forEach( anomaly -> anomalies.put( anomaly.point(), anomaly ) );
        return new Anomalies( anomalies );
    }

    // TODO uncomment or delete. does this policy add any value?
//    private static Anomalies usingRawPolicy( List<Point> points, Variance variance, Mode mode, double perc )
//    {
//        Map<Point,Anomaly> anomalies = IntStream.range( 1, points.size() )
//                .mapToObj( i -> toAnomaly( points.get( i - 1 ), points.get( i ), variance, mode, perc ) )
//                .filter( Optional::isPresent )
//                .map( Optional::get )
//                .collect( toMap( Anomaly::point, anomaly -> anomaly ) );
//        return new Anomalies( anomalies );
//    }

    private static Anomalies usingGlobalPolicy( List<Point> points, Variance variance, Mode mode, double perc )
    {
        Map<Point,Anomaly> anomalies = new HashMap<>();
        Point prev = points.get( 0 );
        for ( int i = 1; i < points.size(); i++ )
        {
            Point point = points.get( i );
            Optional<Anomaly> anomaly = toAnomaly( prev, point, variance, mode, perc );
            if ( anomaly.isPresent() )
            {
                anomalies.put( anomaly.get().point(), anomaly.get() );
                prev = point;
            }
        }
        return new Anomalies( anomalies );
    }

    private static Optional<Anomaly> toAnomaly( Point prev, Point curr, Variance variance, Mode mode, double perc )
    {
        Anomaly.AnomalyType anomalyType = toAnomalyType( prev, curr, variance, mode, perc );

        return (anomalyType == Anomaly.AnomalyType.NONE)
               ? Optional.empty()
               : Optional.of( new Anomaly( prev, curr, anomalyType, calcChange( prev, curr ) ) );
    }

    private static double calcChange( Point prev, Point curr )
    {
        return (prev.value() > curr.value())
               ? prev.value() / curr.value()
               : curr.value() / prev.value();
    }

    private static Anomaly.AnomalyType toAnomalyType( Point prev, Point curr, Variance variance, Mode mode,
                                                      double perc )
    {
        prev = prev.convertTo( NANOSECONDS, mode );
        curr = curr.convertTo( NANOSECONDS, mode );
        double adjustedVariance = VARIANCE_BUFFER * convertTo(
                variance.diffAtPercentile( 50 ),
                variance.unit(),
                NANOSECONDS,
                mode );
        double change = calcChange( prev, curr );

        if ( prev.value() - curr.value() > adjustedVariance && change > perc )
        {
            return (mode.equals( Mode.THROUGHPUT )) ? REGRESSION : IMPROVEMENT;
        }
        else if ( curr.value() - prev.value() > adjustedVariance && change > perc )
        {
            return (mode.equals( Mode.THROUGHPUT )) ? IMPROVEMENT : REGRESSION;
        }
        else
        {
            return Anomaly.AnomalyType.NONE;
        }
    }

    private final Map<Point,Anomaly> anomalies;

    private Anomalies( Map<Point,Anomaly> anomalies )
    {
        this.anomalies = anomalies;
    }

    List<Anomaly> anomalies()
    {
        return anomalies.values().stream().sorted( new DateComparator() ).collect( toList() );
    }

    Optional<Anomaly> anomalyFor( Point point )
    {
        return (anomalies.containsKey( point )) ? Optional.of( anomalies.get( point ) ) : Optional.empty();
    }

    Anomalies since( Date date )
    {
        Map<Point,Anomaly> recentAnomalies = anomalies.keySet().stream()
                                                      .filter( p -> date.before( new Date( p.date() ) ) )
                                                      .collect( toMap( p -> p, anomalies::get ) );
        return new Anomalies( recentAnomalies );
    }

    @Override
    public String toString()
    {
        return anomalies().toString();
    }

    private static class DateComparator implements Comparator<Anomaly>
    {
        @Override
        public int compare( Anomaly o1, Anomaly o2 )
        {
            return Long.compare( o1.point().date(), o2.point().date() );
        }
    }
}
