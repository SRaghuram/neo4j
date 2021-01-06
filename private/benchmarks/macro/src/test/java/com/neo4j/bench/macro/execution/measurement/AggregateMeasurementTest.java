/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.measurement;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class AggregateMeasurementTest
{
    @TempDir
    protected Path temporaryFolder;

    @Test
    public void shouldCalculateAggregate()
    {
        double[] measurements = new double[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        AggregateMeasurement aggregate = AggregateMeasurement.calculateFrom( measurements );

        assertThat( aggregate.percentile( 0.0D ), equalTo( 1.0D ) );
        assertThat( aggregate.percentile( 0.1D ), equalTo( 2.0D ) );
        assertThat( aggregate.percentile( 0.2D ), equalTo( 3.0D ) );
        assertThat( aggregate.percentile( 0.3D ), equalTo( 4.0D ) );
        assertThat( aggregate.percentile( 0.4D ), equalTo( 5.0D ) );
        assertThat( aggregate.percentile( 0.5D ), equalTo( 6.0D ) );
        assertThat( aggregate.percentile( 0.6D ), equalTo( 7.0D ) );
        assertThat( aggregate.percentile( 0.7D ), equalTo( 8.0D ) );
        assertThat( aggregate.percentile( 0.8D ), equalTo( 9.0D ) );
        assertThat( aggregate.percentile( 0.9D ), equalTo( 10.0D ) );
        assertThat( aggregate.percentile( 1.0D ), equalTo( 10.0D ) );

        assertThat( aggregate.mean(), equalTo( 5.5D ) );
        assertThat( aggregate.min(), equalTo( 1.0D ) );
        assertThat( aggregate.median(), equalTo( 6.0D ) );
        assertThat( aggregate.max(), equalTo( 10.0D ) );
    }

    @Test
    public void shouldCalculateSingleResultAggregate()
    {
        double[] measurements = new double[]{1};
        AggregateMeasurement aggregate = AggregateMeasurement.calculateFrom( measurements );

        assertThat( aggregate.percentile( 0.0D ), equalTo( 1.0D ) );
        assertThat( aggregate.percentile( 0.1D ), equalTo( 1.0D ) );
        assertThat( aggregate.percentile( 0.2D ), equalTo( 1.0D ) );
        assertThat( aggregate.percentile( 0.3D ), equalTo( 1.0D ) );
        assertThat( aggregate.percentile( 0.4D ), equalTo( 1.0D ) );
        assertThat( aggregate.percentile( 0.5D ), equalTo( 1.0D ) );
        assertThat( aggregate.percentile( 0.6D ), equalTo( 1.0D ) );
        assertThat( aggregate.percentile( 0.7D ), equalTo( 1.0D ) );
        assertThat( aggregate.percentile( 0.8D ), equalTo( 1.0D ) );
        assertThat( aggregate.percentile( 0.9D ), equalTo( 1.0D ) );
        assertThat( aggregate.percentile( 1.0D ), equalTo( 1.0D ) );

        assertThat( aggregate.mean(), equalTo( 1D ) );
        assertThat( aggregate.min(), equalTo( 1.0D ) );
        assertThat( aggregate.median(), equalTo( 1.0D ) );
        assertThat( aggregate.max(), equalTo( 1.0D ) );
    }
}
