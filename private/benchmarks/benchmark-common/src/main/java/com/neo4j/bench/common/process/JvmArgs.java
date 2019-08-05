/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.process;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.neo4j.bench.common.results.ForkDirectory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.neo4j.bench.common.util.Args.splitArgs;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class JvmArgs
{

    private static final Pattern MEMORY_SETTING = Pattern.compile( "-X(ms|mx|ss)(\\d+)(k|K|m|M|g|G)" );
    private static final Pattern BOOLEAN_ARGUMENT = Pattern.compile( "-XX:(\\+|-)([^=]*)" );
    private static final Pattern VALUE_ARGUMENT = Pattern.compile( "-XX:(.*)=.*" );

    public static List<String> standardArgs( ForkDirectory forkDirectory )
    {
        // Other options:
        //      -XX:+HeapDumpAfterFullGC             : Creates heap dump file after full GC
        //      -XX:+HeapDumpBeforeFullGC            : Creates heap dump file before full GC
        //      -XX:+PrintHeapAtGC                   : Print heap layout before and after each GC  <--  very log spammy
        //      -XX:+PrintTLAB                       : Print TLAB allocation statistics
        //      -XX:+PrintReferenceGC                : Print times for weak/soft/JNI/etc reference processing during STW pause
        //      -XX:+PrintGCCause
        //      -XX:+PrintClassHistogramBeforeFullGC : Prints class histogram before full GC
        //      -XX:+PrintClassHistogramAfterFullGC  : Prints class histogram after full GC
        //      -XX:+PrintGCTimeStamps               : Print timestamps for each GC event (seconds count from start of JVM)  <-- use PrintGCDateStamps instead
        return Lists.newArrayList(
                "-XX:+HeapDumpOnOutOfMemoryError",                   // Creates heap dump in out-of-memory condition
                "-XX:HeapDumpPath=" + forkDirectory.toAbsolutePath() // Specifies path to save heap dumps
        );
    }

    public static List<String> jvmArgsFromString( String jvmArgs )
    {
        return Arrays.asList( splitArgs( jvmArgs, " " ) );
    }

    public static String jvmArgsToString( List<String> jvmArgs )
    {
        return String.join( " ", jvmArgs );
    }

    public static JvmArgs from( List<String> jvmArgs )
    {
        return new JvmArgs( jvmArgs );
    }

    private final List<String> jvmArgs;

    private JvmArgs( List<String> jvmArgs )
    {
        this.jvmArgs = jvmArgs;
    }

    /**
     * Intelligent way of setting JVM arguments, first it tries to find if it is already set,
     * it does so by first finding argument name.
     * If yes replaces old value and sets new value in the same position. If not, appends
     * new JVM argument at the end of list.
     * @param jvmArg
     * @return
     */
    public JvmArgs set( String jvmArg )
    {
        List<String> list = jvmArgs.stream().map( mapArg( jvmArg ) ).collect( toList() );
        if ( !list.contains( jvmArg ) )
        {
            list.add( jvmArg );
        }
        return new JvmArgs( list );
    }

    public List<String> toArgs()
    {
        return ImmutableList.copyOf( jvmArgs );
    }

    private static Function<String,String> mapArg( String newJvmArg )
    {

        Optional<String> newArgName = extractArgName( newJvmArg );
        // if arg names are equal return new jvm arg, else return old one
        // if we cannot parse argument, throw error
        return oldJvmArg -> newArgName.map( arg -> extractArgName( oldJvmArg ).filter( arg::equals ).map( k -> newJvmArg ).orElseGet( () -> oldJvmArg ) )
                .get();
    }

    private static Optional<String> extractArgName( String jvmArg )
    {
        Matcher matcher = MEMORY_SETTING.matcher( jvmArg );
        if ( matcher.matches() )
        {
            return Optional.of( matcher.group( 1 ) );
        }

        matcher = BOOLEAN_ARGUMENT.matcher( jvmArg );
        if ( matcher.matches() )
        {
            return Optional.of( matcher.group( 2 ) );
        }

        matcher = VALUE_ARGUMENT.matcher( jvmArg );
        if ( matcher.matches() )
        {
            return Optional.of( matcher.group(1) );
        }

        throw new IllegalArgumentException( format( "don't know how to handle %s JVM argument", jvmArg ) );
    }
}
