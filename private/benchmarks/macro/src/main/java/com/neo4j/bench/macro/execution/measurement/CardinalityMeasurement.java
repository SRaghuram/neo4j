/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.measurement;

import com.neo4j.bench.model.model.PlanOperator;

import java.util.ArrayList;
import java.util.List;

/**
 * Computes (variations of) q-error for logical plans.
 * <p>
 * See: Moerkotte, Guido, Thomas Neumann, and Gabriele Steidl. "Preventing Bad Plans by Bounding the Impact of Cardinality Estimation Errors." (2009).
 */
public abstract class CardinalityMeasurement
{
    public static CardinalityMeasurement atRoot( double qErrorMax )
    {
        return new RootCardinalityMeasurement( qErrorMax );
    }

    public static CardinalityMeasurement atWorst( double qErrorMax )
    {
        return new MaxCardinalityMeasurement( qErrorMax );
    }

    public static CardinalityMeasurement geometricMean( double qErrorMax )
    {
        return new GeometricMeanCardinalityMeasurement( qErrorMax );
    }

    public abstract double calculateError( PlanOperator plan );

    /**
     * Calculate the 'q-error' for a single operator.
     *
     * The q-error can be seen as a percentage error between the real and estimated cardinality.
     * For example, a q-error of 1.2 represents that the estimate is either 20% more than the real cardinality or 20% less.
     * q-error is both multiplicative and symmetric, so can be used to convey information about error propagation.
     * By calculating the q-error for every operator in a plan, we can combine these errors to convey information about the whole query plan.
     */
    protected double qError( PlanOperator plan, double qErrorMax )
    {
        double estimatedRows = plan.estimatedRows().doubleValue();
        double rows = plan.rows().map( Long::doubleValue ).orElse( 0.0D );
        return qError( estimatedRows, rows, qErrorMax );
    }

    private double qError( double a, double b, double qErrorMax )
    {
        double largerValue = Math.max( a, b );
        double smallerValue = Math.min( a, b );
        double qError = (0.0D == smallerValue && 0.0D == largerValue)
                        ? 1.0D
                        : largerValue / smallerValue;
        return Math.min( qError, qErrorMax );
    }

    /**
     * Reports the q-error at the root operator, i.e., Produce Results
     */
    private static class RootCardinalityMeasurement extends CardinalityMeasurement
    {
        private final double qErrorMax;

        private RootCardinalityMeasurement( double qErrorMax )
        {
            this.qErrorMax = qErrorMax;
        }

        @Override
        public double calculateError( PlanOperator plan )
        {
            return qError( plan, qErrorMax );
        }
    }

    /**
     * Reports the maximum q-error present in the evaluation plan.
     * The maximum q-error is an indicator for the degree of error propagation in a given evaluation plan,
     * the operator with the largest q-error is the operator that introduces the most error in a plan.
     */
    private static class MaxCardinalityMeasurement extends CardinalityMeasurement
    {
        private final double qErrorMax;

        private MaxCardinalityMeasurement( double qErrorMax )
        {
            this.qErrorMax = qErrorMax;
        }

        @Override
        public double calculateError( PlanOperator plan )
        {
            double qError = qError( plan, qErrorMax );
            if ( plan.children().isEmpty() )
            {
                return qError;
            }
            else
            {
                double maxChildQError = plan.children().stream().mapToDouble( this::calculateError ).max().getAsDouble();
                return Math.max( qError, maxChildQError );
            }
        }
    }

    /**
     * As q-errors are multiplicative, we are able to take a geo-metric mean of the error values.
     * As opposed to the natural mean, the geometric mean is less influenced by extreme outliers.
     * As opposed to only considering the maximum q-error, the geometric mean conveys information about the errors of all operators in the query plan.
     */
    private static class GeometricMeanCardinalityMeasurement extends CardinalityMeasurement
    {
        private final double qErrorMax;

        private GeometricMeanCardinalityMeasurement( double qErrorMax )
        {
            this.qErrorMax = qErrorMax;
        }

        @Override
        public double calculateError( PlanOperator plan )
        {
            List<Double> errors = new ArrayList<>();
            populateErrors( plan, errors );
            double product = errors.stream().reduce( 1D, ( left, right ) -> left * right );
            return Math.min( Math.pow( product, 1.0D / errors.size() ), qErrorMax );
        }

        private void populateErrors( PlanOperator plan, List<Double> errors )
        {
            errors.add( qError( plan, qErrorMax ) );
            plan.children().forEach( child -> populateErrors( child, errors ) );
        }
    }
}
