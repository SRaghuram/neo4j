/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.jfr;

import org.openjdk.jmc.common.IMCStackTrace;
import org.openjdk.jmc.common.item.IItem;
import org.openjdk.jmc.common.item.IItemCollection;
import org.openjdk.jmc.common.item.IItemIterable;
import org.openjdk.jmc.common.item.IMemberAccessor;
import org.openjdk.jmc.common.item.IType;
import org.openjdk.jmc.common.item.ItemFilters;
import org.openjdk.jmc.common.unit.IQuantity;
import org.openjdk.jmc.flightrecorder.JfrAttributes;
import org.openjdk.jmc.flightrecorder.JfrLoaderToolkit;
import org.openjdk.jmc.flightrecorder.jdk.JdkAttributes;
import org.openjdk.jmc.flightrecorder.jdk.JdkTypeIDs;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toCollection;

/**
 * Simple utility class, which converts any JFR recording into stack collapsed file
 * which you can later convert into SVG using FlameGraphs.
 *
 */
public class Main
{
    public static void main( String[] args ) throws Exception
    {
        if ( args.length != 1 )
        {
            System.out.println( "this command requires one argument, no more, no less" );
        }
        File file = new File( args[0] );

        IItemCollection loadEvents = JfrLoaderToolkit.loadEvents( file );

        Iterator<IItemIterable> itemIterables = loadEvents
                .apply( ItemFilters.type( JdkTypeIDs.ALLOC_INSIDE_TLAB, JdkTypeIDs.ALLOC_OUTSIDE_TLAB ) )
                .iterator();

        while ( itemIterables.hasNext() )
        {
            IItemIterable itermIterable = itemIterables.next();
            Iterator<IItem> items = itermIterable.iterator();
            while ( items.hasNext() )
            {
                IItem item = items.next();
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
                    System.out.println( stackTrace.getMember( item ).getFrames().stream()
                            .map( m -> m.getMethod().getType().getFullName() + "#" + m.getMethod().getMethodName() )
                            .collect( collectingAndThen( toCollection( ArrayList::new ), lst -> {
                                Collections.reverse( lst );
                                return lst.stream();
                            } ) ).collect( Collectors.joining( ";" ) ) + " "
                            + allocationSize.getMember( item ).longValue() );
                }
            }
        }
    }
}
