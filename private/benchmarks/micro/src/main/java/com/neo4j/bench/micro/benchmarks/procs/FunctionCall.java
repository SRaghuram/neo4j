/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.procs;

import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;

import java.util.concurrent.TimeUnit;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

import static com.neo4j.bench.micro.Main.run;

@OutputTimeUnit( TimeUnit.MICROSECONDS )
public class FunctionCall extends AbstractProceduresBenchmark
{
    @Override
    protected void afterDatabaseStart( DataGeneratorConfig config )
    {
        try
        {
            super.afterDatabaseStart( config );
            procedures.registerFunction( TestFunctions.class );
            QualifiedName qualifiedName = new QualifiedName( new String[]{"tester"}, "function" );
            UserFunctionHandle handle = procedures.function( qualifiedName );
            token = handle.id();
        }
        catch ( KernelException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public AnyValue testFunction( RNGState rngState ) throws ProcedureException
    {
        return procedures.callFunction(
                context,
                token,
                new AnyValue[]{Values.longValue( rngState.rng.nextLong() )} );
    }

    public static class TestFunctions
    {
        @UserFunction( name = "tester.function" )
        public long function( @Name( "value" ) Long value )
        {
            return value;
        }
    }

    public static void main( String... methods ) throws Exception
    {
        run( FunctionCall.class, methods );
    }
}
