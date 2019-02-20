/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Plan
{
    public static final String REQUESTED_PLANNER = "requested_planner";
    public static final String USED_PLANNER = "used_planner";
    public static final String DEFAULT_PLANNER = "default_planner";
    public static final String REQUESTED_RUNTIME = "requested_runtime";
    public static final String USED_RUNTIME = "used_runtime";
    public static final String DEFAULT_RUNTIME = "default_runtime";
    public static final String CYPHER_VERSION = "cypher_version";

    private final String requestedPlanner; // e.g. 'idp'
    private final String usedPlanner;
    private final String defaultPlanner;
    private final String requestedRuntime; // e.g. 'interpreted'
    private final String usedRuntime;
    private final String defaultRuntime;
    private final String cypherVersion; // e.g. 'CYPHER 3.1'
    private final PlanCompilationMetrics planCompilationMetrics;
    private final PlanTree planTree;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public Plan()
    {
        this( "-1", "-1", "-1", "-1", "-1", "-1", "-1", new PlanCompilationMetrics(), new PlanTree() );
    }

    public Plan(
            String requestedPlanner,
            String usedPlanner,
            String defaultPlanner,
            String requestedRuntime,
            String usedRuntime,
            String defaultRuntime,
            String cypherVersion,
            PlanCompilationMetrics planCompilationMetrics,
            PlanTree planTree )
    {
        this.requestedPlanner = requestedPlanner;
        this.usedPlanner = usedPlanner;
        this.defaultPlanner = defaultPlanner;
        this.requestedRuntime = requestedRuntime;
        this.usedRuntime = usedRuntime;
        this.defaultRuntime = defaultRuntime;
        this.cypherVersion = cypherVersion;
        this.planCompilationMetrics = planCompilationMetrics;
        this.planTree = planTree;
    }

    public PlanCompilationMetrics planCompilationMetrics()
    {
        return planCompilationMetrics;
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
        return map;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        Plan plan = (Plan) o;
        return Objects.equals( requestedPlanner, plan.requestedPlanner ) &&
               Objects.equals( usedPlanner, plan.usedPlanner ) &&
               Objects.equals( defaultPlanner, plan.defaultPlanner ) &&
               Objects.equals( requestedRuntime, plan.requestedRuntime ) &&
               Objects.equals( usedRuntime, plan.usedRuntime ) &&
               Objects.equals( defaultRuntime, plan.defaultRuntime ) &&
               Objects.equals( cypherVersion, plan.cypherVersion ) &&
               Objects.equals( planCompilationMetrics, plan.planCompilationMetrics ) &&
               Objects.equals( planTree, plan.planTree );
    }

    @Override
    public int hashCode()
    {
        return Objects
                .hash( requestedPlanner, usedPlanner, defaultPlanner, requestedRuntime, usedRuntime, defaultRuntime,
                        cypherVersion, planCompilationMetrics, planTree );
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
               ", planCompilationMetrics=" + planCompilationMetrics +
               ", planTree=" + planTree +
               '}';
    }
}
