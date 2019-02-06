/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.values;

import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.config.ParamValues;
import com.neo4j.bench.micro.data.ValueGeneratorFun;
import com.neo4j.bench.micro.data.ValueGeneratorUtil.Range;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.nio.charset.StandardCharsets;

import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;

import static com.neo4j.bench.micro.Main.run;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.defaultRangeFor;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.randGeneratorFor;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class StringFunctions extends AbstractValuesBenchmark
{
    @Override
    public String description()
    {
        return "Benchmark string functions";
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }

    @ParamValues(
            allowed = {STR_SML, STR_BIG},
            base = {STR_BIG} )
    @Param( {} )
    public String StringFunctions_type;

    @ParamValues(
            allowed = {"UTF8", "STRING"},
            base = {"UTF8", "STRING"} )
    @Param( {} )
    public String StringFunctions_encoding;

    @State( Scope.Thread )
    public static class ThreadState
    {
        private TextValue nextValue;

        @Setup
        public void setUp( StringFunctions benchmarkState, RNGState rngState ) throws InterruptedException
        {
            nextValue = createValue( benchmarkState, rngState );
        }

        private TextValue createValue( StringFunctions benchmarkState, RNGState rngState )
        {
            Range range = defaultRangeFor( benchmarkState.StringFunctions_type );
            ValueGeneratorFun fun = randGeneratorFor(
                    benchmarkState.StringFunctions_type,
                    range.min(),
                    range.max(),
                    false ).create();
            String next = (String) fun.next( rngState.rng );
            switch ( benchmarkState.StringFunctions_encoding )
            {
            case "UTF8":
                byte[] bytes = next.getBytes( StandardCharsets.UTF_8 );
                return Values.utf8Value( bytes );
            case "STRING":
                return Values.stringValue( next );
            default:
                throw new IllegalArgumentException( "Unknown encoding: " + benchmarkState.StringFunctions_encoding );
            }
        }
    }

    @Benchmark
    @CompilerControl( CompilerControl.Mode.DONT_INLINE )
    @BenchmarkMode( {Mode.AverageTime} )
    public int length( ThreadState threadState )
    {
        return threadState.nextValue.length();
    }

    @Benchmark
    @CompilerControl( CompilerControl.Mode.DONT_INLINE )
    @BenchmarkMode( {Mode.AverageTime} )
    public TextValue trim( ThreadState threadState )
    {
        return threadState.nextValue.trim();
    }

    @Benchmark
    @CompilerControl( CompilerControl.Mode.DONT_INLINE )
    @BenchmarkMode( {Mode.AverageTime} )
    public TextValue substring( ThreadState threadState )
    {
        return threadState.nextValue.substring( 3, 7 );
    }

    @Benchmark
    @CompilerControl( CompilerControl.Mode.DONT_INLINE )
    @BenchmarkMode( {Mode.AverageTime} )
    public ListValue split( ThreadState threadState )
    {
        return threadState.nextValue.split( "a" );
    }

    @Benchmark
    @CompilerControl( CompilerControl.Mode.DONT_INLINE )
    @BenchmarkMode( {Mode.AverageTime} )
    public TextValue reverse( ThreadState threadState )
    {
        return threadState.nextValue.reverse();
    }

    @Benchmark
    @CompilerControl( CompilerControl.Mode.DONT_INLINE )
    @BenchmarkMode( {Mode.AverageTime} )
    public TextValue replace( ThreadState threadState )
    {
        return threadState.nextValue.replace( "a", "b" );
    }

    public static void main( String... methods ) throws Exception
    {
        run( StringFunctions.class, methods );
    }
}
