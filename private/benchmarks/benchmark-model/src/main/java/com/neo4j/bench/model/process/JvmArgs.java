/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.process;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.lang3.StringUtils;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
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
    private static final Pattern AGENTLIB = Pattern.compile( "-(?<argname>agentlib):.*" );

    private static final List<Pattern> PATTERNS =
            asList( MEMORY_SETTING, JVM_SETTING, BOOLEAN_ARGUMENT, VALUE_ARGUMENT, PROPERTY, AGENTLIB );

    @JsonCreator
    public static JvmArgs parse( String jvmArgs )
    {
        List<String> args = new ArrayList<>();
        CharacterIterator characters = new StringCharacterIterator( jvmArgs.trim() );
        StringBuilder builder = new StringBuilder();
        boolean quoted = false;

        for ( char c = characters.first(); c != CharacterIterator.DONE; c = characters.next() )
        {
            // we have found space and we are not in between quotes
            // so we will try to add JVM argument
            if ( c == ' ' && !quoted )
            {
                builder = addArgFromBuilder( args, builder );
            }
            // beginning or end of quoted string
            else if ( c == '\"' )
            {
                quoted = !quoted;
            }
            else
            {
                builder.append( c );
            }
        }
        // if there is JVM argument left in a string builder
        // add it to JVM arguments
        addArgFromBuilder( args, builder );
        return JvmArgs.from( args );
    }

    public static JvmArgs empty()
    {
        return new JvmArgs();
    }

    public static JvmArgs from( List<String> jvmArgs )
    {
        return new JvmArgs( jvmArgs );
    }

    public static JvmArgs from( String... rawJvmArgs )
    {
        JvmArgs jvmArgs = JvmArgs.empty();
        for ( String jvmArg : rawJvmArgs )
        {
            jvmArgs = jvmArgs.set( jvmArg );
        }
        return jvmArgs;
    }

    private static StringBuilder addArgFromBuilder( List<String> args, StringBuilder builder )
    {
        String jvmArg = builder.toString();
        if ( StringUtils.isNotBlank( jvmArg ) )
        {
            args.add( jvmArg );
            builder = new StringBuilder();
        }
        return builder;
    }

    private final List<String> jvmArgs;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public JvmArgs()
    {
        this( new ArrayList<>() );
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
     *
     * @param jvmArg
     * @return new instance of JvmArgs
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

    public JvmArgs addAll( List<String> newJvmArgs )
    {
        Objects.requireNonNull( newJvmArgs );

        JvmArgs allJvmArgs = this;
        for ( String jvmArg : newJvmArgs )
        {
            allJvmArgs = allJvmArgs.set( jvmArg );
        }

        return allJvmArgs;
    }

    public JvmArgs merge( JvmArgs with )
    {
        return this.addAll( with.toArgs() );
    }

    public List<String> toArgs()
    {
        return new ArrayList<>( jvmArgs );
    }

    @JsonValue
    public String toArgsString()
    {
        return String.join( " ", jvmArgs );
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
        if ( obj == null || getClass() != obj.getClass() )
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
        return oldJvmArg ->
        {
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
                       .map( m -> m.group( ARGNAME_CAPTURING_GROUP ) )
                       .findFirst()
                       .orElseThrow( () -> new IllegalArgumentException( format( "Don't know how to handle JVM argument: '%s'", jvmArg ) ) );
    }

    public static JvmArgs standardArgs()
    {
        return empty();
    }
}
