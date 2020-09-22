/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Plan
{
    private static final String REQUESTED_PLANNER = "requested_planner";
    private static final String USED_PLANNER = "used_planner";
    private static final String DEFAULT_PLANNER = "default_planner";
    private static final String REQUESTED_RUNTIME = "requested_runtime";
    private static final String USED_RUNTIME = "used_runtime";
    private static final String DEFAULT_RUNTIME = "default_runtime";
    private static final String CYPHER_VERSION = "cypher_version";
    private static final String QUERY = "cypher_query";

    private final String requestedPlanner; // e.g. 'idp'
    private final String usedPlanner;
    private final String defaultPlanner;
    private final String requestedRuntime; // e.g. 'interpreted'
    private final String usedRuntime;
    private final String defaultRuntime;
    private final String cypherVersion; // e.g. 'CYPHER 3.1'
    private final PlanTree planTree;
    private final String queryString;

    @JsonCreator
    public Plan(
            @JsonProperty( "requestedPlanner" ) String requestedPlanner,
            @JsonProperty( "usedPlanner" ) String usedPlanner,
            @JsonProperty( "defaultPlanner" ) String defaultPlanner,
            @JsonProperty( "requestedRuntime" ) String requestedRuntime,
            @JsonProperty( "usedRuntime" ) String usedRuntime,
            @JsonProperty( "defaultRuntime" ) String defaultRuntime,
            @JsonProperty( "cypherVersion" ) String cypherVersion,
            @JsonProperty( "queryString" ) String queryString,
            @JsonProperty( "planTree" ) PlanTree planTree )
    {
        this.requestedPlanner = requestedPlanner;
        this.usedPlanner = usedPlanner;
        this.defaultPlanner = defaultPlanner;
        this.requestedRuntime = requestedRuntime;
        this.usedRuntime = usedRuntime;
        this.defaultRuntime = defaultRuntime;
        this.cypherVersion = cypherVersion;
        this.queryString = queryString;
        this.planTree = planTree;
    }

    public PlanTree planTree()
    {
        return planTree;
    }

    public Map<String,Object> asMap()
    {
        Map<String,Object> map = new HashMap<>();
        map.put( REQUESTED_PLANNER, requestedPlanner );
        map.put( USED_PLANNER, usedPlanner );
        map.put( DEFAULT_PLANNER, defaultPlanner );
        map.put( REQUESTED_RUNTIME, requestedRuntime );
        map.put( USED_RUNTIME, usedRuntime );
        map.put( DEFAULT_RUNTIME, defaultRuntime );
        map.put( CYPHER_VERSION, cypherVersion );
        map.put( QUERY, queryString );
        return map;
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
        return "Plan{" +
               "requestedPlanner='" + requestedPlanner + '\'' +
               ", usedPlanner='" + usedPlanner + '\'' +
               ", defaultPlanner='" + defaultPlanner + '\'' +
               ", requestedRuntime='" + requestedRuntime + '\'' +
               ", usedRuntime='" + usedRuntime + '\'' +
               ", defaultRuntime='" + defaultRuntime + '\'' +
               ", cypherVersion='" + cypherVersion + '\'' +
               ", planTree=" + planTree +
               '}';
    }
}
