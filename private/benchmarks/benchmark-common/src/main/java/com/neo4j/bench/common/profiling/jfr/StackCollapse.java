/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.jfr;

import org.apache.commons.lang3.tuple.Pair;
import org.openjdk.jmc.common.IMCFrame;
import org.openjdk.jmc.common.IMCStackTrace;
import org.openjdk.jmc.common.item.IItem;
import org.openjdk.jmc.common.item.IItemCollection;
import org.openjdk.jmc.common.item.IItemIterable;
import org.openjdk.jmc.common.item.IMemberAccessor;
import org.openjdk.jmc.common.item.IType;
import org.openjdk.jmc.common.unit.IQuantity;
import org.openjdk.jmc.common.util.FormatToolkit;
import org.openjdk.jmc.flightrecorder.JfrAttributes;
import org.openjdk.jmc.flightrecorder.JfrLoaderToolkit;
import org.openjdk.jmc.flightrecorder.jdk.JdkAttributes;
import org.openjdk.jmc.flightrecorder.jdk.JdkFilters;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.summingLong;
import static java.util.stream.Collectors.toList;

/**
 * Transforms stack traces into collapsed stacks,
 * which can be later read by <code>flamegraph.pl</code>
 * from <a href="https://github.com/brendangregg/FlameGraph">Brenand's Gregg Flamegraph</a>.
 */
public class StackCollapse implements AutoCloseable
{
    private final PrintWriter printWriter;
    private final Stream.Builder<Pair<String,Long>> streamBuilder;

    /**
     * Collapse stacks for memory allocation.
     *
     * @param jfrRecording input JFR recording
     * @param stackCollapsedFile output location for generated collapsed stacks file
     * @throws Exception
     */
    public static void forMemoryAllocation( Path jfrRecording, Path stackCollapsedFile ) throws Exception
    {
        IItemCollection allocationEvents = JfrLoaderToolkit.loadEvents( jfrRecording.toFile() ).apply( JdkFilters.ALLOC_ALL );
        try ( StackCollapse stackCollapse = new StackCollapse( stackCollapsedFile.toFile() ) )
        {
            for ( IItemIterable event : allocationEvents )
            {
                for ( IItem item : event )
                {
                    IType<?> type = item.getType();
                    @SuppressWarnings( "unchecked" )
                    IMemberAccessor<IQuantity,Object> allocationSize =
                            (IMemberAccessor<IQuantity,Object>) JdkAttributes.ALLOCATION_SIZE.getAccessor( type );
                    @SuppressWarnings( "unchecked" )
                    IMemberAccessor<IMCStackTrace,Object> stackTrace =
                            (IMemberAccessor<IMCStackTrace,Object>) JfrAttributes.EVENT_STACKTRACE.getAccessor( type );
                    IMCStackTrace mcStackTrace = stackTrace.getMember( item );
                    if ( mcStackTrace != null /*native stack*/ )
                    {
                        stackCollapse.addStackTrace( mcStackTrace.getFrames(),
                                                     allocationSize.getMember( item ).longValue() );
                    }
                }
            }
        }
    }

    private StackCollapse( File file ) throws FileNotFoundException
    {
        this.printWriter = new PrintWriter( file );
        streamBuilder = Stream.builder();
    }

    /**
     * Add next stack trace and value associated with stack trace.
     *
     * @param stackTrace
     * @param value
     */
    private void addStackTrace( List<? extends IMCFrame> stackTrace, Long value )
    {
        streamBuilder.add(
                Pair.of(
                        stackTrace.stream()
                                  .map( StackCollapse::toString )
                                  .collect( collectingAndThen( toList(), StackCollapse::reverse ) )
                                  .collect( joining( ";" ) ),
                        value ) );
    }

    @Override
    public void close()
    {
        try
        {
            streamBuilder.build()
                         .collect( groupingBy( Pair::getLeft, summingLong( Pair::getRight ) ) )
                         .forEach( ( stack, sum ) -> printWriter.println( format( "%s %d", stack, sum ) ) );
        }
        finally
        {
            printWriter.close();
        }
    }

    private static String toString( IMCFrame frame )
    {
        return FormatToolkit.getHumanReadable( frame.getMethod(),
                                               false,       // show return value
                                               false, // show return value package
                                               true,         // show class name
                                               true,   // show class package name
                                               true,         // show arguments
                                               true ); // show arguments package
    }

    private static <T> Stream<T> reverse( List<T> lst )
    {
        Collections.reverse( lst );
        return lst.stream();
    }
}
