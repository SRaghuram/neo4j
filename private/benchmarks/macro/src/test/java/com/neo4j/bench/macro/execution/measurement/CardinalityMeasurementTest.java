/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.measurement;

import com.neo4j.bench.model.model.PlanOperator;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static java.lang.Math.pow;
import static java.lang.Math.round;
import static java.lang.Math.sqrt;
import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class CardinalityMeasurementTest
{
    private static final double MAX_ERROR = 1_000_000;
    private static final CardinalityMeasurement ROOT = CardinalityMeasurement.atRoot( MAX_ERROR );
    private static final CardinalityMeasurement WORST = CardinalityMeasurement.atWorst( MAX_ERROR );
    private static final CardinalityMeasurement GEO_MEAN = CardinalityMeasurement.geometricMean( MAX_ERROR );

    @Test
    public void shouldWorkForFinite()
    {
        PlanOperator p1 = plan( 1, (long) Double.POSITIVE_INFINITY );
        assertThat( ROOT.calculateError( p1 ), equalTo( MAX_ERROR ) );
        assertThat( WORST.calculateError( p1 ), equalTo( MAX_ERROR ) );
        assertThat( GEO_MEAN.calculateError( p1 ), equalTo( MAX_ERROR ) );

        PlanOperator p2 = plan( (long) Double.POSITIVE_INFINITY, 1 );
        assertThat( ROOT.calculateError( p2 ), equalTo( MAX_ERROR ) );
        assertThat( WORST.calculateError( p2 ), equalTo( MAX_ERROR ) );
        assertThat( GEO_MEAN.calculateError( p2 ), equalTo( MAX_ERROR ) );
    }

    @Test
    public void shouldWorkForOneOperator()
    {
        PlanOperator p1 = plan( 1, 1 );
        assertThat( ROOT.calculateError( p1 ), equalTo( 1D ) );
        assertThat( WORST.calculateError( p1 ), equalTo( 1D ) );
        assertThat( GEO_MEAN.calculateError( p1 ), equalTo( sqrt( 1D ) ) );

        PlanOperator p2 = plan( 2, 1 );
        assertThat( ROOT.calculateError( p2 ), equalTo( 2D ) );
        assertThat( WORST.calculateError( p2 ), equalTo( 2D ) );
        assertThat( GEO_MEAN.calculateError( p2 ), equalTo( 2D ) );

        PlanOperator p3 = plan( 1, 2 );
        assertThat( ROOT.calculateError( p3 ), equalTo( 2D ) );
        assertThat( WORST.calculateError( p3 ), equalTo( 2D ) );
        assertThat( GEO_MEAN.calculateError( p3 ), equalTo( 2D ) );

        PlanOperator p4 = plan( 1, 0 );
        assertThat( ROOT.calculateError( p4 ), equalTo( MAX_ERROR ) );
        assertThat( WORST.calculateError( p4 ), equalTo( MAX_ERROR ) );
        assertThat( GEO_MEAN.calculateError( p4 ), equalTo( MAX_ERROR ) );

        PlanOperator p5 = plan( 0, 1 );
        assertThat( ROOT.calculateError( p5 ), equalTo( MAX_ERROR ) );
        assertThat( WORST.calculateError( p5 ), equalTo( MAX_ERROR ) );
        assertThat( GEO_MEAN.calculateError( p5 ), equalTo( MAX_ERROR ) );
    }

    @Test
    public void shouldWorkForLinearPlan()
    {
        /*
         p1
         |
         p2
         |
         p3
         |
         p4
         |
         p5
         */

        PlanOperator p5 = plan( 3, 1 );
        PlanOperator p4 = plan( 1, 1, p5 );
        PlanOperator p3 = plan( 1, round( MAX_ERROR ), p4 );
        PlanOperator p2 = plan( 2, 1, p3 );
        PlanOperator p1 = plan( 1, 1, p2 );

        assertThat( ROOT.calculateError( p1 ), equalTo( 1D ) );
        assertThat( ROOT.calculateError( p2 ), equalTo( 2D ) );
        assertThat( ROOT.calculateError( p3 ), equalTo( MAX_ERROR ) );
        assertThat( ROOT.calculateError( p4 ), equalTo( 1D ) );
        assertThat( ROOT.calculateError( p5 ), equalTo( 3D ) );

        assertThat( WORST.calculateError( p1 ), equalTo( MAX_ERROR ) );
        assertThat( WORST.calculateError( p2 ), equalTo( MAX_ERROR ) );
        assertThat( WORST.calculateError( p3 ), equalTo( MAX_ERROR ) );
        assertThat( WORST.calculateError( p4 ), equalTo( 3D ) );
        assertThat( WORST.calculateError( p5 ), equalTo( 3D ) );

        assertThat( GEO_MEAN.calculateError( p1 ), equalTo( pow( 1D * 2D * MAX_ERROR * 1D * 3D, 1 / 5D ) ) );
        assertThat( GEO_MEAN.calculateError( p2 ), equalTo( pow( 2D * MAX_ERROR * 1D * 3D, 1 / 4D ) ) );
        assertThat( GEO_MEAN.calculateError( p3 ), equalTo( pow( MAX_ERROR * 1D * 3D, 1 / 3D ) ) );
        assertThat( GEO_MEAN.calculateError( p4 ), equalTo( pow( 1D * 3D, 1 / 2D ) ) );
        assertThat( GEO_MEAN.calculateError( p5 ), equalTo( 3D ) );
    }

    @Test
    public void shouldWorkForBranchingPlan()
    {
        /*
         p1
         |
         p2
         | \
         p3 p4
            | \
            p5 p6
            | \
            p7 p8
         */

        PlanOperator p8 = plan( 3, 1 );
        PlanOperator p7 = plan( 4, 1 );
        PlanOperator p6 = plan( 8, 1 );
        PlanOperator p5 = plan( 3, 1, p7, p8 );
        PlanOperator p4 = plan( 1, 1, p5, p6 );
        PlanOperator p3 = plan( 1, 4 );
        PlanOperator p2 = plan( 2, 1, p3, p4 );
        PlanOperator p1 = plan( 1, 1, p2 );

        assertThat( ROOT.calculateError( p1 ), equalTo( 1D ) );
        assertThat( ROOT.calculateError( p2 ), equalTo( 2D ) );
        assertThat( ROOT.calculateError( p3 ), equalTo( 4D ) );
        assertThat( ROOT.calculateError( p4 ), equalTo( 1D ) );
        assertThat( ROOT.calculateError( p5 ), equalTo( 3D ) );
        assertThat( ROOT.calculateError( p6 ), equalTo( 8D ) );
        assertThat( ROOT.calculateError( p7 ), equalTo( 4D ) );
        assertThat( ROOT.calculateError( p8 ), equalTo( 3D ) );

        assertThat( WORST.calculateError( p1 ), equalTo( 8D ) );
        assertThat( WORST.calculateError( p2 ), equalTo( 8D ) );
        assertThat( WORST.calculateError( p3 ), equalTo( 4D ) );
        assertThat( WORST.calculateError( p4 ), equalTo( 8D ) );
        assertThat( WORST.calculateError( p5 ), equalTo( 4D ) );
        assertThat( WORST.calculateError( p6 ), equalTo( 8D ) );
        assertThat( WORST.calculateError( p7 ), equalTo( 4D ) );
        assertThat( WORST.calculateError( p8 ), equalTo( 3D ) );

        assertThat( GEO_MEAN.calculateError( p1 ), equalTo( pow( 1D * 2D * 4D * 1D * 3D * 8D * 4D * 3D, 1 / 8D ) ) );
        assertThat( GEO_MEAN.calculateError( p2 ), equalTo( pow( 2D * 4D * 1D * 3D * 8D * 4D * 3D, 1 / 7D ) ) );
        assertThat( GEO_MEAN.calculateError( p3 ), equalTo( 4D ) );
        assertThat( GEO_MEAN.calculateError( p4 ), equalTo( pow( 1D * 3D * 8D * 4D * 3D, 1 / 5D ) ) );
        assertThat( GEO_MEAN.calculateError( p5 ), equalTo( pow( 3D * 4D * 3D, 1 / 3D ) ) );
        assertThat( GEO_MEAN.calculateError( p6 ), equalTo( 8D ) );
        assertThat( GEO_MEAN.calculateError( p7 ), equalTo( 4D ) );
        assertThat( GEO_MEAN.calculateError( p8 ), equalTo( 3D ) );
    }

    private PlanOperator plan( long estimate, long actual, PlanOperator... children )
    {
        return new PlanOperator( 1, "operatorType", estimate, 0L, actual, emptyList(), Arrays.asList( children ) );
    }
}
