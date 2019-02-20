/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.procs;

import com.neo4j.bench.micro.config.ParamValues;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;

import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.CallableUserAggregationFunction.Aggregator;
import org.neo4j.kernel.api.proc.QualifiedName;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;

import static com.neo4j.bench.micro.Main.run;

public class AggregationFunctionCall extends AbstractProceduresBenchmark
{
    @ParamValues(
            allowed = {"1", "100", "10000", "1000000"},
            base = {"1", "100", "10000", "1000000"} )
    @Param( {} )
    public long AggregationFunctionCall_rows;

    @Override
    protected void afterDatabaseStart()
    {
        try
        {
            super.afterDatabaseStart();
            procedures.registerAggregationFunction( TestAggregation.class );
            qualifiedName = new QualifiedName( new String[]{"tester"}, "aggregation" );
        }
        catch ( KernelException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public long testAggregation() throws ProcedureException
    {
        Aggregator aggregator = procedures.createAggregationFunction( context, qualifiedName );
        for ( long i = 0; i < AggregationFunctionCall_rows; i++ )
        {
            aggregator.update( new Object[]{i} );
        }

        return (long) aggregator.result();
    }

    public static class TestAggregation
    {
        @UserAggregationFunction( name = "tester.aggregation" )
        public CountAggregator count()
        {
            return new CountAggregator();
        }
    }

    public static class CountAggregator
    {
        private long count;

        @UserAggregationUpdate()
        public void update( @Name( "in" ) long in )
        {
            count += 1L;
        }

        @UserAggregationResult
        public long result()
        {
            return count;
        }
    }

    public static void main( String... methods ) throws Exception
    {
        run( AggregationFunctionCall.class, methods );
    }
}
