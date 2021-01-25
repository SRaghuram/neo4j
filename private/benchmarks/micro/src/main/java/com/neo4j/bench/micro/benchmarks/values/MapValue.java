/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.values;

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.RNGState;
import org.apache.commons.lang3.StringUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.VirtualValues;

import static com.neo4j.bench.micro.Main.run;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@BenchmarkEnabled( true )
@OutputTimeUnit( NANOSECONDS )
public class MapValue extends AbstractValuesBenchmark
{
    @Override
    public String description()
    {
        return "Benchmark MapValue";
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    @ParamValues(
            allowed = {"4", "40", "400"},
            base = {"4", "40", "400"} )
    @Param( {} )
    public int size;

    @ParamValues(
            allowed = {"50", "75", "100"},
            base = {"50", "75", "100"} )
    @Param( {} )
    public int fraction;

    @State( Scope.Thread )
    public static class ThreadState
    {
        private org.neo4j.values.virtual.MapValue mapValue;
        private int bound;
        private String[] keys;

        @Setup
        public void setUp( MapValue benchmarkState, RNGState rngState )
        {
            mapValue = createValue( benchmarkState, rngState );
        }

        AnyValue get( RNGState rngState )
        {
            int index = rngState.rng.nextInt( bound );
            if ( bound >= keys.length )
            {
                return mapValue.get( "MISSING_KEY" );
            }
            else
            {
                return mapValue.get( keys[index] );
            }
        }

        private org.neo4j.values.virtual.MapValue createValue( MapValue benchmarkState, RNGState rngState )
        {

            bound = (int) (benchmarkState.size * (100.0 / benchmarkState.fraction));
            keys = new String[benchmarkState.size];
            AnyValue[] values = new AnyValue[benchmarkState.size];
            for ( int i = 0; i < keys.length; i++ )
            {
                String base = Long.toHexString( rngState.rng.nextLong() );
                int length = rngState.rng.nextInt( 3, 10 );
                if ( base.length() > length )
                {
                    keys[i] = base.substring( 0, length ) + "-" + i;
                }
                else
                {
                    String padding =  "a".repeat( length - base.length() );
                    keys[i] = base + padding + "-" + i;
                }

                values[i] = Values.utf8Value( keys[i] );
            }

            return VirtualValues.map( keys, values );
        }
    }

    @Benchmark
    @CompilerControl( CompilerControl.Mode.DONT_INLINE )
    @BenchmarkMode( {Mode.AverageTime} )
    public AnyValue get( ThreadState threadState, RNGState rngState )
    {
        return threadState.get( rngState );
    }

    @Benchmark
    @CompilerControl( CompilerControl.Mode.DONT_INLINE )
    @BenchmarkMode( {Mode.AverageTime} )
    public void foreach( ThreadState threadState, Blackhole bh )
    {
        threadState.mapValue.foreach( ( k, v ) ->
                                      {
                                          bh.consume( k );
                                          bh.consume( v );
                                      } );
    }

    public static void main( String... methods ) throws Exception
    {
        run( MapValue.class, methods );
    }
}
