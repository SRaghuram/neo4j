/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.procedures.detection;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static com.neo4j.bench.client.model.Benchmark.Mode.THROUGHPUT;
import static com.neo4j.bench.procedures.detection.TestSequences.gradualDegradation;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import static java.util.stream.Collectors.joining;

public class AnomaliesTest
{
    private static final String NEO4J_SERIES = "3.1";

    @Test
    @Disabled
    // TODO un ignore
    public void test() throws Exception
    {
        Series series = new Series( NEO4J_SERIES, gradualDegradation( 1 ), THROUGHPUT );
        Variance variance = Variance.calculateFor( series );
        double percentageTolerated = 1.15;

        Anomalies anomalies = Anomalies.calculateFor( series, variance, percentageTolerated );

        // TODO remove
        System.out.println( variance );
        System.out.println( anomalies.anomalies().stream().map( Anomaly::toString ).collect( joining( "\n" ) ) );

        assertThat( anomalies.anomalies().size(), equalTo( 1 ) );
    }
}
