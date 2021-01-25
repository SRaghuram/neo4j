/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.procs;

import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregator;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;
import org.neo4j.values.AnyValue;

import static com.neo4j.bench.micro.Main.run;
import static org.neo4j.values.storable.Values.longValue;

public class AggregationFunctionCall extends AbstractProceduresBenchmark
{
    @ParamValues(
            allowed = {"1", "100", "10000", "1000000"},
            base = {"1", "100", "10000", "1000000"} )
    @Param( {} )
    public long rows;

    @Override
    protected void afterDatabaseStart( DataGeneratorConfig config )
    {
        try
        {
            super.afterDatabaseStart( config );
            procedures.registerAggregationFunction( TestAggregation.class );
            QualifiedName qualifiedName = new QualifiedName( new String[]{"tester"}, "aggregation" );
            UserFunctionHandle handle = procedures.aggregationFunction( qualifiedName );
            token = handle.id();
        }
        catch ( KernelException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public AnyValue testAggregation() throws ProcedureException
    {
        UserAggregator aggregator = procedures.createAggregationFunction( context, token );
        for ( long i = 0; i < rows; i++ )
        {
            aggregator.update( new AnyValue[]{longValue( i )} );
        }

        return aggregator.result();
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
