/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.process;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.neo4j.bench.common.results.ForkDirectory;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.neo4j.bench.common.util.Args.splitArgs;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@JsonTypeInfo( use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_ARRAY )
public class JvmArgs
{

    private static final Pattern MEMORY_SETTING = Pattern.compile( "-X(?<argname>ms|mx|ss)(\\d+)(k|K|m|M|g|G)" );
    private static final Pattern BOOLEAN_ARGUMENT = Pattern.compile( "-XX:(\\+|-)(?<argname>[^=]*)" );
    private static final Pattern VALUE_ARGUMENT = Pattern.compile( "-XX:(?<argname>.*)=.*" );

    private static final List<Pattern> PATTERNS = Arrays.asList( MEMORY_SETTING,BOOLEAN_ARGUMENT, VALUE_ARGUMENT );

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

    @JsonCreator
    public static JvmArgs from( String[] jvmArgs )
    {
        return new JvmArgs( Arrays.asList( jvmArgs ) );
    }

    private final List<String> jvmArgs;

    private JvmArgs( List<String> jvmArgs )
    {
        this.jvmArgs = requireNonNull( jvmArgs );
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

    @JsonValue
    public String[] asArray()
    {
        return toArgs().toArray( new String[] {} );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( jvmArgs );
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( !(obj instanceof JvmArgs) )
        {
            return false;
        }
        JvmArgs other = (JvmArgs) obj;
        return Objects.equals( jvmArgs, other.jvmArgs );
    }

    private static Function<String,String> mapArg( String newJvmArg )
    {

        String newArgName = extractArgName( newJvmArg );
        // if arg names are equal return new jvm arg, else return old one
        // if we cannot parse argument, throw error
        return oldJvmArg -> {
            String oldArgName = extractArgName( oldJvmArg );
            if ( oldArgName.equals( newArgName ) )
            {
                return newJvmArg;
            }
            return oldJvmArg;
        };
    }

    private static String extractArgName( String jvmArg )
    {

        return PATTERNS.stream()
                .map( p -> p.matcher( jvmArg ) )
                .filter( Matcher::matches )
                .map( m -> m.group( "argname") )
                .findFirst()
                .orElseThrow( () -> new IllegalArgumentException( format( "don't know how to handle %s JVM argument", jvmArg ) ) );
    }

}
