/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.values;

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.RNGState;
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

import java.util.function.Supplier;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.impl.locking.ResourceIds;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static com.neo4j.bench.micro.data.ValueGeneratorUtil.BYTE;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.BYTE_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.FLT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.FLT_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.POINT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.SHORT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.SHORT_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.defaultRangeFor;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.randGeneratorFor;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class HashCode extends AbstractValuesBenchmark
{
    @Override
    public String description()
    {
        return "Benchmarks hashcode implementations of Values";
    }

    @Override
    public boolean isThreadSafe()
    {
        // Technically, this benchmark is completely thread-safe. However, there is honestly no point in benchmarking
        // these functions with multiple concurrent threads, versus a single-threaded benchmark. So we mark this
        // benchmark as not thread-safe to save the time that would otherwise be wasted on benchmarking this with
        // 8 concurrent threads.
        return false;
    }

    @ParamValues( allowed = {BYTE, SHORT, STR_SML, STR_BIG, INT, LNG, FLT, DBL, POINT, BYTE_ARR,
            SHORT_ARR, INT_ARR, LNG_ARR, DBL_ARR, FLT_ARR, STR_BIG_ARR, STR_SML_ARR},
            base = {STR_SML, STR_BIG, POINT, BYTE_ARR, LNG_ARR, DBL_ARR}
    )
    @Param( {} )
    public String type;

    private static final int ARBITRARY_UNIMPORTANT_IMAGINARY_LABEL_ID = 13;
    private static final int ARBITRARY_UNIMPORTANT_IMAGINARY_PROPERTY_KEY_ID = 11;

    @State( Scope.Thread )
    public static class ThreadState
    {
        private Supplier<Value> nextValue;

        @Setup
        public void setUp( HashCode benchmarkState, RNGState rngState ) throws InterruptedException
        {
            nextValue = createHashCodeSupplier( benchmarkState, rngState );
        }

        private Supplier<Value> createHashCodeSupplier( HashCode benchmarkState, RNGState rngState )
        {
            String type = benchmarkState.type;
            Range range = defaultRangeFor( type );
            ValueGeneratorFun fun = randGeneratorFor(
                    type,
                    range.min(),
                    range.max(),
                    integral( type ) ).create();
            Object value = fun.next( rngState.rng );

            // A NOTE ABOUT STRINGS: String objects cache their hash code values. If we were to reuse the String
            // objects for the string benchmarks, then we would get a vastly under-counted average-time result.
            // The hash code inside String is produced and cached on the first call to hashCode() on that particular
            // String object instance. It turns out that the new String(String) constructor copies both the reference
            // to the internal char-array, and the cached hash code. This makes this constructor very cheap.
            // It is our luck that the hash code is cached lazily. This means that as long as we don't call hashCode()
            // on the original "unhashed" instances, we can keep making copies of them with the String(String)
            // constructor, and each copy will get the uninitialised hash code value. This way, we get to measure the
            // cost of computing the hash code every time the benchmark runs.

            switch ( type )
            {
            case BYTE:
                return () -> Values.byteValue( (byte) value );
            case SHORT:
                return () -> Values.shortValue( (short) value );
            case INT:
                return () -> Values.intValue( (int) value );
            case LNG:
                return () -> Values.longValue( (long) value );
            case FLT:
                return () -> Values.floatValue( (float) value );
            case DBL:
                return () -> Values.doubleValue( (double) value );
            case STR_SML:
            case STR_BIG:
                String unhashedString = (String) value;
                //noinspection StringOperationCanBeSimplified
                return () -> Values.stringValue( new String( unhashedString ) );
            case POINT:
                return () -> (PointValue) value;
            case BYTE_ARR:
                return () -> Values.byteArray( (byte[]) value );
            case SHORT_ARR:
                return () -> Values.shortArray( (short[]) value );
            case INT_ARR:
                return () -> Values.intArray( (int[]) value );
            case LNG_ARR:
                return () -> Values.longArray( (long[]) value );
            case FLT_ARR:
                return () -> Values.floatArray( (float[]) value );
            case DBL_ARR:
                return () -> Values.doubleArray( (double[]) value );
            case STR_SML_ARR:
            case STR_BIG_ARR:
                String[] unhashedStringArray = (String[]) value;
                String[] hashingStringArray = new String[unhashedStringArray.length];
                return () ->
                {
                    for ( int i = 0; i < unhashedStringArray.length; i++ )
                    {
                        //noinspection StringOperationCanBeSimplified
                        hashingStringArray[i] = new String( unhashedStringArray[i] );
                    }
                    return Values.stringArray( hashingStringArray );
                };
            default:
                throw new UnsupportedOperationException( "Cannot create array of " + type );
            }
        }

        private boolean integral( String type )
        {
            return !(type.equals( STR_BIG ) ||
                     type.equals( STR_SML ) ||
                     type.equals( STR_BIG_ARR ) ||
                     type.equals( STR_SML_ARR ));
        }
    }

    @Benchmark
    @CompilerControl( CompilerControl.Mode.DONT_INLINE )
    @BenchmarkMode( {Mode.AverageTime} )
    public int hashCode( ThreadState threadState )
    {
        return threadState.nextValue.get().hashCode();
    }

    @Benchmark
    @CompilerControl( CompilerControl.Mode.DONT_INLINE )
    @BenchmarkMode( {Mode.AverageTime} )
    public long indexEntryHashCode4xx( ThreadState threadState )
    {
        return ResourceIds.indexEntryResourceId(
                ARBITRARY_UNIMPORTANT_IMAGINARY_LABEL_ID, IndexQuery.exact(
                        ARBITRARY_UNIMPORTANT_IMAGINARY_PROPERTY_KEY_ID,
                        threadState.nextValue.get() ) );
    }
}
