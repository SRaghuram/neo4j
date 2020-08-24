/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.Maps.immutableEntry;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class ParameterizedProfiler
{

    private static final Pattern PROFILER_PARAMETERS_REGEXP = Pattern.compile( "([a-zA-Z$_]\\w*)(\\((.*?)\\))?" );
    private static final Pattern KEY_VALUE_REGEXP = Pattern.compile( "([^,]+?)=([^,]+)" );

    public static List<ParameterizedProfiler> parse( String profilersParametersArg )
    {

        if ( StringUtils.isBlank( profilersParametersArg ) )
        {
            return Collections.emptyList();
        }

        Matcher matcher = PROFILER_PARAMETERS_REGEXP.matcher( profilersParametersArg );
        List<ParameterizedProfiler> allProfilersParameters = new ArrayList<>();
        while ( matcher.find() )
        {
            String profilerType = matcher.group( 1 );
            // try to parse, parameters
            String parameters = matcher.group( 3 );
            Map<String,List<String>> parametersMap;
            if ( parameters == null )
            {
                parametersMap = emptyMap();
            }
            else
            {
                parametersMap = parseParameters( parameters );
            }

            allProfilersParameters.add( new ParameterizedProfiler( ProfilerType.valueOf( profilerType ), parametersMap ) );

            // check if there is more to be matched
            // if reached end of string that's fine, break
            if ( profilersParametersArg.length() == matcher.end() )
            {
                break;
            }
            else
            {
                char matcherEndChar = profilersParametersArg.charAt( matcher.end() );
                if ( matcherEndChar != ',' && !Character.isWhitespace( matcherEndChar ) )
                {
                    throw new IllegalStateException( format( "unexpected \"%s\" character in %s", matcherEndChar, profilersParametersArg ) );
                }
            }
        }

        return allProfilersParameters;
    }

    private static Map<String,List<String>> parseParameters( String parameters )
    {

        if ( StringUtils.isBlank( parameters ) )
        {
            return emptyMap();
        }
        Matcher matcher = KEY_VALUE_REGEXP.matcher( parameters );
        Map<String,List<String>> parametersMap = new HashMap<>();
        int matcherEnd = -1;
        while ( matcher.find() )
        {
            matcherEnd = matcher.end();
            String key = matcher.group( 1 );
            String value = matcher.group( 2 );
            parametersMap.computeIfAbsent( key, k -> new ArrayList<>() ).add( value );
            // check if there is more to be matched
            // if reached end of string that's fine, break
            if ( parameters.length() == matcherEnd )
            {
                break;
            }
            else
            {
                char matcherEndChar = parameters.charAt( matcherEnd );
                if ( matcherEndChar != ',' )
                {
                    throw new IllegalStateException( format( "unexpected \"%s\" character in %s", matcherEndChar, parameters ) );
                }
            }
        }

        // check if we have matched everything
        if ( parameters.length() != matcherEnd )
        {
            throw new IllegalStateException( format( "key-value pair \"%s\" not well-formed %s", parameters.substring( matcherEnd ), parameters ) );
        }
        return parametersMap.entrySet()
                            .stream()
                            .map( entry -> immutableEntry( entry.getKey(), new ArrayList<>( entry.getValue() ) ) )
                            .collect( toMap(
                                    Map.Entry::getKey, Map.Entry::getValue ) );
    }

    private final ProfilerType profilerType;
    private final Map<String,List<String>> parameters;

    @JsonCreator
    ParameterizedProfiler( @JsonProperty( "profilerType" ) ProfilerType profilerType,
                           @JsonProperty( "parameters" ) Map<String,List<String>> parameters )
    {
        super();
        this.profilerType = profilerType;
        this.parameters = parameters;
    }

    public static ParameterizedProfiler defaultProfiler( ProfilerType profilers )
    {
        return defaultProfilers( Arrays.asList( profilers ) ).get( 0 );
    }

    public static List<ParameterizedProfiler> defaultProfilers( ProfilerType... profilers )
    {
        return defaultProfilers( Arrays.asList( profilers ) );
    }

    public static List<ParameterizedProfiler> defaultProfilers( List<ProfilerType> profilers )
    {
        return profilers.stream().map( profiler -> new ParameterizedProfiler( profiler, emptyMap() ) ).collect( toList() );
    }

    public static List<ParameterizedProfiler> internalProfilers( List<ParameterizedProfiler> profilers )
    {
        return profilers.stream().filter( profiler -> profiler.profilerType().isInternal() ).collect( toList() );
    }

    public static void assertInternal( List<ParameterizedProfiler> profilers )
    {
        ProfilerType.assertInternal( profilerTypes( profilers ) );
    }

    public static List<ProfilerType> profilerTypes( List<ParameterizedProfiler> profilers )
    {
        return profilers.stream().map( ParameterizedProfiler::profilerType ).collect( toList() );
    }

    public ProfilerType profilerType()
    {
        return profilerType;
    }

    public Map<String,List<String>> parameters()
    {
        return parameters;
    }

    public static String serialize( List<ParameterizedProfiler> profilers )
    {
        return profilers.stream().map( ParameterizedProfiler::serialize ).collect( joining( "," ) );
    }

    public String serialize()
    {
        if ( parameters.isEmpty() )
        {
            return profilerType.name();
        }
        return format( "%s(%s)", profilerType.name(), parametersToArgs() );
    }

    private String parametersToArgs()
    {
        return parameters.entrySet().stream()
                         .flatMap( entry -> entry.getValue().stream().map( value -> String.format( "%s=%s", entry.getKey(), value ) ) )
                         .collect( joining( "," ) );
    }

    @Override
    public boolean equals( Object that )
    {
        return EqualsBuilder.reflectionEquals( this, that );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }

    @Override
    public String toString()
    {
        return serialize();
    }
}
