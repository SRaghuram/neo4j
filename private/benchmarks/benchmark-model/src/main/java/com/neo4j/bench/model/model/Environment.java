/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import oshi.SystemInfo;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;

public class Environment
{

    public static Environment from( Instance... instances )
    {
        return new Environment( Arrays.stream( instances ).collect( groupingBy( identity(), counting() ) ) );
    }

    public static Environment local()
    {
        SystemInfo systemInfo = new SystemInfo();
        long totalMemory = systemInfo.getHardware().getMemory().getTotal();
        int availableCores = systemInfo.getHardware().getProcessor().getLogicalProcessorCount();
        return new Environment( new HashMap<>( singletonMap(
                new Instance( currentServer(),
                              Instance.Kind.Server,
                              currentOperatingSystem(),
                              availableCores,
                              totalMemory )
                , 1L ) ) );
    }

    @JsonSerialize( keyUsing = Instance.InstanceKeySerializer.class )
    @JsonDeserialize( keyUsing = Instance.InstanceKeyDeserializer.class )
    private final Map<Instance,Long> instances;

    @JsonCreator
    public Environment( @JsonProperty( "instances" ) Map<Instance,Long> instances )
    {
        this.instances = requireNonNull( instances );
    }

    public Map<Instance,Long> instances()
    {
        return instances;
    }

    public Set<Instance> distinctInstances()
    {
        return instances.keySet();
    }

    @Override
    public boolean equals( Object o )
    {
        return EqualsBuilder.reflectionEquals( o, this );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString( this );
    }

    private static String currentOperatingSystem()
    {
        return SystemUtils.OS_NAME + ", " + SystemUtils.OS_VERSION;
    }

    private static String currentServer()
    {
        try
        {
            return InetAddress.getLocalHost().getHostName();
        }
        catch ( UnknownHostException e )
        {
            throw new RuntimeException( e );
        }
    }

    public Collection<Map<String,Object>> toMap()
    {
        return instances.entrySet()
                        .stream()
                        .map( entry ->
                                      ImmutableMap.<String,Object>builder()
                                              .putAll( entry.getKey().toMap() )
                                              .put( "count", entry.getValue() )
                                              .build() )
                        .collect( toSet() );
    }
}
