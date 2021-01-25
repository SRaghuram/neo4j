/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.jfr;

import com.neo4j.bench.common.profiling.StackCollapse;
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

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static java.util.stream.Collectors.joining;

/**
 * Transforms stack traces into collapsed stacks,
 * which can be later read by <code>flamegraph.pl</code>
 * from <a href="https://github.com/brendangregg/FlameGraph">Brenand's Gregg Flamegraph</a>.
 */
public class JfrMemoryStackCollapse implements StackCollapse
{
    /**
     * Collapse stacks for memory allocation.
     *
     * @param jfrRecording input JFR recording
     * @throws Exception
     */
    public static JfrMemoryStackCollapse forMemoryAllocation( Path jfrRecording ) throws Exception
    {
        IItemCollection allocationEvents = JfrLoaderToolkit.loadEvents( jfrRecording.toFile() ).apply( JdkFilters.ALLOC_ALL );
        JfrMemoryStackCollapse stackCollapse = new JfrMemoryStackCollapse();
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
        return stackCollapse;
    }

    private final Map<List<? extends IMCFrame>,Long> stackTraceAggregates = new HashMap<>();

    /**
     * Add next stack trace and value associated with stack trace.
     */
    private void addStackTrace( List<? extends IMCFrame> stackTrace, Long value )
    {
        stackTraceAggregates.compute( stackTrace, ( st, sum ) -> (null == sum) ? value : sum + value );
    }

    @Override
    public void forEachStackTrace( BiConsumer<String,Long> fun )
    {
        stackTraceAggregates.forEach( ( stackTrace, sum ) ->
                                      {
                                          Collections.reverse( stackTrace );
                                          String stackTraceString = stackTrace.stream().map( JfrMemoryStackCollapse::frameToString ).collect( joining( ";" ) );
                                          fun.accept( stackTraceString, sum );
                                      } );
    }

    private static String frameToString( IMCFrame frame )
    {
        return FormatToolkit.getHumanReadable( frame.getMethod(),
                                               false,       // show return value
                                               false, // show return value package
                                               true,         // show class name
                                               true,   // show class package name
                                               true,         // show arguments
                                               true ); // show arguments package
    }
}
