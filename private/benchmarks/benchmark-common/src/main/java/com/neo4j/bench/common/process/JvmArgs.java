/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.process;

import com.google.common.collect.Lists;
import com.neo4j.bench.common.results.ForkDirectory;
import org.apache.commons.lang3.StringUtils;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class JvmArgs
{

    private static final String ARGNAME_CAPTURING_GROUP = "argname";

    private static final Pattern MEMORY_SETTING = Pattern.compile( "-X(?<argname>ms|mx|ss)(\\d+)(k|K|m|M|g|G)" );
    private static final Pattern JVM_SETTING = Pattern.compile( "-X(?<argname>[^X][^:]+)(:.+)?" );
    private static final Pattern BOOLEAN_ARGUMENT = Pattern.compile( "-XX:(\\+|-)(?<argname>[^=]+)" );
    private static final Pattern VALUE_ARGUMENT = Pattern.compile( "-XX:(?<argname>[^=]+)=.*" );
    private static final Pattern PROPERTY = Pattern.compile( "-D(?<argname>[^=]+)(=.+)?" );

    private static final List<Pattern> PATTERNS =
            asList( MEMORY_SETTING, JVM_SETTING, BOOLEAN_ARGUMENT, VALUE_ARGUMENT, PROPERTY );

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
        ArrayList<String> args = new ArrayList<>();

        StringCharacterIterator iter = new StringCharacterIterator( jvmArgs );

        StringBuilder builder = new StringBuilder();
        boolean quoted = false;

        for ( char c = iter.first(); ; c = iter.next() )
        {
            if ( c == CharacterIterator.DONE )
            {
                builder = addArg( args, builder );
                break;

            }
            if ( c == ' ' && !quoted )
            {
                builder = addArg( args, builder );
                continue;
            }
            if ( c == '\"' )
            {
                quoted = !quoted;
            }
            builder.append( c );
        }

        return args;
    }

    private static StringBuilder addArg( ArrayList<String> args, StringBuilder builder )
    {
        String str = builder.toString().trim();
        if ( StringUtils.isNotBlank( str ) )
        {
            args.add( str );
            builder = new StringBuilder();
        }
        return builder;
    }

    public static String jvmArgsToString( List<String> jvmArgs )
    {
        return String.join( " ", jvmArgs );
    }

    public static JvmArgs from( List<String> jvmArgs )
    {
        return new JvmArgs( jvmArgs );
    }

    public static JvmArgs from( String[] jvmArgs )
    {
        return new JvmArgs( Arrays.asList( jvmArgs ) );
    }

    private final List<String> jvmArgs;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public JvmArgs()
    {
        this( new ArrayList<String>() );
    }

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
        List<String> args = jvmArgs.stream().map( mapArg( jvmArg ) ).collect( toList() );
        if ( !args.contains( jvmArg ) )
        {
            args.add( jvmArg );
        }
        return new JvmArgs( args );
    }

    public List<String> toArgs()
    {
        return new ArrayList<String>( jvmArgs );
    }

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

    @Override
    public String toString()
    {
        return String.valueOf( jvmArgs );
    }

    /**
     * If argument names are equal return new JVM argument value,
     * else return old one. If we cannot parse argument, throw an error,
     *
     * @param newJvmArg
     * @return
     */
    private static Function<String,String> mapArg( String newJvmArg )
    {
        String newArgName = extractArgName( newJvmArg );
        return oldJvmArg -> {
            String oldArgName = extractArgName( oldJvmArg );
            if ( oldArgName.equals( newArgName ) )
            {
                return newJvmArg;
            }
            return oldJvmArg;
        };
    }

    /**
     * JVM argument can come in many flavors.
     * In order to extract JVM argument name we go through list
     * of possible patterns, and extract argument name from
     * first pattern that matches.
     *
     * @param jvmArg
     * @return
     */
    private static String extractArgName( String jvmArg )
    {
        return PATTERNS.stream()
                .map( p -> p.matcher( jvmArg ) )
                .filter( Matcher::matches )
                .map( m -> m.group( ARGNAME_CAPTURING_GROUP) )
                .findFirst()
                .orElseThrow( () -> new IllegalArgumentException( format( "Don't know how to handle JVM argument: '%s'", jvmArg ) ) );
    }

}
