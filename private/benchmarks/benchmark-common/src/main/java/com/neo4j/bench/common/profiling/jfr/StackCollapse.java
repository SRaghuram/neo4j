/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.jfr;

import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.lang3.tuple.Pair;
import org.openjdk.jmc.common.IMCFrame;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.summingLong;
import static java.util.stream.Collectors.toCollection;

/**
 * Transforms stack traces into collapsed stacks,
 * which can be later read by <code>flamegraph.pl</code>
 * from <a href="https://github.com/brendangregg/FlameGraph">Brenand's Gregg Flamegraph</a>.
 *
 */
public class StackCollapse implements AutoCloseable
{
    private final PrintWriter printWriter;
    private final Stream.Builder<Pair<String,Long>> streamBuilder;

    public StackCollapse()
    {
        // prevents System.out from being closed
        printWriter = new PrintWriter( new CloseShieldOutputStream( System.out ) );
        streamBuilder = Stream.builder();
    }

    /**
     * Add next stack trace and value associated with stack trace.
     *
     * @param frames
     * @param value
     */
    public void addStackTrace( List<? extends IMCFrame> frames, LongSupplier value )
    {
        streamBuilder.add(
                Pair.of(
                        frames.stream()
                            .map( StackCollapse::toString )
                            .collect( collectingAndThen( toCollection( ArrayList::new ), StackCollapse::reverse ) )
                            .collect( joining( ";" ) ),
                        value.getAsLong() ) );
    }

    @Override
    public void close() throws Exception
    {
        try
        {
            streamBuilder.build()
            .collect( groupingBy( Pair::getLeft, summingLong( Pair::getRight ) ) )
            .forEach( ( k, v ) -> printWriter.println( format( "%s %d", k, v ) ) );
        }
        finally
        {
            printWriter.close();
        }
    }

    private static String toString( IMCFrame frame )
    {
        return format("%s#%s",frame.getMethod().getType().getFullName(), frame.getMethod().getMethodName() );
    }

    private static <T> Stream<T> reverse( List<T> lst )
    {
        Collections.reverse( lst );
        return lst.stream();
    }

}
