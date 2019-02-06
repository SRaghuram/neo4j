/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.procs;

import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.config.ParamValues;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;

import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static com.neo4j.bench.micro.Main.run;

@OutputTimeUnit( TimeUnit.MICROSECONDS )
public class ProcedureCall extends AbstractProceduresBenchmark
{
    @ParamValues(
            allowed = {"1", "100", "10000", "1000000"},
            base = {"1", "100", "10000", "1000000"} )
    @Param( {} )
    public long ProcedureCall_rows;

    @Override
    protected void afterDatabaseStart()
    {
        try
        {
            super.afterDatabaseStart();
            procedures.registerProcedure( TestProcedure.class );
            QualifiedName qualifiedName = new QualifiedName( new String[]{"tester"}, "procedure" );
            ProcedureHandle handle = procedures.procedure( qualifiedName );
            token = handle.id();
        }
        catch ( KernelException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public long testProcedure() throws ProcedureException
    {
        RawIterator<Object[],ProcedureException> iterator = procedures.callProcedure(
                context,
                token,
                new Object[]{ProcedureCall_rows},
                DUMMY_TRACKER );

        int count = 0;
        while ( iterator.hasNext() )
        {
            iterator.next();
            count++;
        }
        return count;
    }

    public static class TestProcedure
    {
        private static final ThreadLocal<SplittableRandom> THREAD_LOCAL_RNG =
                ThreadLocal.withInitial( () -> RNGState.newRandom( 42L ) );

        @Procedure( name = "tester.procedure" )
        public Stream<LongValue> procedure( @Name( "value" ) Long value )
        {
            SplittableRandom rng = THREAD_LOCAL_RNG.get();
            return LongStream.range( 0, value ).mapToObj( l -> new LongValue( rng.nextLong() ) );
        }
    }

    public static class LongValue
    {
        public Long value;

        public LongValue( Long value )
        {
            this.value = value;
        }
    }

    public static void main( String... methods ) throws Exception
    {
        run( ProcedureCall.class, methods );
    }
}
