/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.procedures.detection;

import com.neo4j.bench.client.model.Neo4j;
import org.junit.Test;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static com.neo4j.bench.client.model.Benchmark.Mode.LATENCY;
import static com.neo4j.bench.client.model.Benchmark.Mode.THROUGHPUT;
import static com.neo4j.bench.client.model.Edition.ENTERPRISE;
import static com.neo4j.bench.procedures.detection.Series.saneUnitFor;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SeriesTest
{
    private static final Neo4j NEO4J = new Neo4j( "7ac43ef", "3.2.1", ENTERPRISE, "3.2", "NEO4J" );
    private static final Point S = new Point( 1, 1, 1, SECONDS, NEO4J );
    private static final Point MS = new Point( 2, 2, 1, MILLISECONDS, NEO4J );
    private static final Point US = new Point( 3, 3, 1, MICROSECONDS, NEO4J );
    private static final Point NS = new Point( 4, 4, 1, NANOSECONDS, NEO4J );

    @Test
    public void shouldComputeSaneUnitForThroughputMode() throws Exception
    {
        List<Point> points;

        String branch = NEO4J.branch();
        assertThat( saneUnitFor( new Series( branch, newArrayList( S ), THROUGHPUT ) ), equalTo( SECONDS ) );
        assertThat( saneUnitFor( new Series( branch, newArrayList( MS ), THROUGHPUT ) ), equalTo( MILLISECONDS ) );
        assertThat( saneUnitFor( new Series( branch, newArrayList( US ), THROUGHPUT ) ), equalTo( MICROSECONDS ) );
        assertThat( saneUnitFor( new Series( branch, newArrayList( NS ), THROUGHPUT ) ), equalTo( NANOSECONDS ) );

        assertThat( saneUnitFor( new Series( branch, newArrayList( S ), LATENCY ) ), equalTo( SECONDS ) );
        assertThat( saneUnitFor( new Series( branch, newArrayList( MS ), LATENCY ) ), equalTo( MILLISECONDS ) );
        assertThat( saneUnitFor( new Series( branch, newArrayList( US ), LATENCY ) ), equalTo( MICROSECONDS ) );
        assertThat( saneUnitFor( new Series( branch, newArrayList( NS ), LATENCY ) ), equalTo( NANOSECONDS ) );

        points = newArrayList(
                S,      // 1 op/ns  <--- highest throughput
                MS,     // 1 op/us
                US,     // 1 op/ms
                NS );   // 1 op/s   <--- highest latency
        assertThat( saneUnitFor( new Series( branch, points, THROUGHPUT ) ), equalTo( NANOSECONDS ) );
        assertThat( saneUnitFor( new Series( branch, points, LATENCY ) ), equalTo( SECONDS ) );
    }
}
