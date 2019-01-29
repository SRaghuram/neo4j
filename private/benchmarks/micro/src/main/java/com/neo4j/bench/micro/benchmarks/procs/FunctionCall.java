package com.neo4j.bench.micro.benchmarks.procs;

import com.neo4j.bench.micro.benchmarks.RNGState;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;

import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.QualifiedName;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import static com.neo4j.bench.micro.Main.run;

@OutputTimeUnit( TimeUnit.MICROSECONDS )
public class FunctionCall extends AbstractProceduresBenchmark
{
    @Override
    protected void afterDatabaseStart()
    {
        try
        {
            super.afterDatabaseStart();
            procedures.registerFunction( TestFunctions.class );
            qualifiedName = new QualifiedName( new String[]{"tester"}, "function" );
        }
        catch ( KernelException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public long testFunction( RNGState rngState ) throws ProcedureException
    {
        return (long) procedures.callFunction(
                context,
                qualifiedName,
                new Object[]{rngState.rng.nextLong()} );
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
