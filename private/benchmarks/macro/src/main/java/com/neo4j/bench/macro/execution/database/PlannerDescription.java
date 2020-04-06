/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.database;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.model.model.Plan;
import com.neo4j.bench.model.model.PlanOperator;
import com.neo4j.bench.model.model.PlanTree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.neo4j.cypher.internal.plandescription.InternalPlanDescription;
import org.neo4j.graphdb.ExecutionPlanDescription;

import static java.util.stream.Collectors.toMap;

public class PlannerDescription
{
    private static final String UNKNOWN = "unknown";
    private static final String PLANNER_IMPL = "planner-impl";
    private static final String RUNTIME_IMPL = "runtime-impl";
    private static final String DB_HITS = "DbHits";
    private static final String VERSION = "version";
    private static final String OPERATOR_TYPE = "operatorType";
    private static final String ESTIMATED_ROWS = "EstimatedRows";
    private static final String ROWS = "Rows";
    private static final String CHILDREN = "children";
    private static final String PLANNER = "planner";
    private static final String RUNTIME = "runtime";
    private static final Set<String> NON_ARGUMENT_MAP_KEYS = Sets.newHashSet( VERSION,
                                                                              OPERATOR_TYPE,
                                                                              ESTIMATED_ROWS,
                                                                              DB_HITS,
                                                                              ROWS,
                                                                              CHILDREN,
                                                                              PLANNER,
                                                                              RUNTIME,
                                                                              PLANNER_IMPL,
                                                                              RUNTIME_IMPL );

    public static PlannerDescription fromResults( ExecutionPlanDescription profileWithPlannerAndRuntime,
                                                  ExecutionPlanDescription explainWithoutPlannerOrRuntime,
                                                  String requestedPlanner,
                                                  String requestedRuntime )
    {
        Map<String,Object> usedArguments = profileWithPlannerAndRuntime.getArguments();
        String usedPlanner = (String) usedArguments.getOrDefault( PLANNER_IMPL, UNKNOWN );
        String usedRuntime = (String) usedArguments.getOrDefault( RUNTIME_IMPL, UNKNOWN );
        Long dbHits = (Long) usedArguments.get( DB_HITS );

        Map<String,Object> defaultArguments = explainWithoutPlannerOrRuntime.getArguments();
        String defaultPlanner = (String) defaultArguments.getOrDefault( PLANNER_IMPL, UNKNOWN );
        String defaultRuntime = (String) defaultArguments.getOrDefault( RUNTIME_IMPL, UNKNOWN );

        return new PlannerDescription( requestedPlanner.toLowerCase(),
                                       usedPlanner.toLowerCase(),
                                       defaultPlanner.toLowerCase(),
                                       requestedRuntime,
                                       usedRuntime.toLowerCase(),
                                       defaultRuntime.toLowerCase(),
                                       profileWithPlannerAndRuntime,
                                       dbHits );
    }

    private final String requestedPlanner;
    private final String usedPlanner;
    private final String defaultPlanner;
    private final String requestedRuntime;
    private final String usedRuntime;
    private final String defaultRuntime;
    private final ExecutionPlanDescription planDescription;
    private final Long dbHits;

    private PlannerDescription( String requestedPlanner,
                                String usedPlanner,
                                String defaultPlanner,
                                String requestedRuntime,
                                String usedRuntime,
                                String defaultRuntime,
                                ExecutionPlanDescription planDescription,
                                Long dbHits )
    {
        this.requestedPlanner = requestedPlanner;
        this.usedPlanner = usedPlanner;
        this.defaultPlanner = defaultPlanner;
        this.requestedRuntime = requestedRuntime;
        this.usedRuntime = usedRuntime;
        this.defaultRuntime = defaultRuntime;
        this.planDescription = planDescription;
        this.dbHits = dbHits;
    }

    public String asciiPlanDescription()
    {
        return planDescription.toString();
    }

    public String requestedPlanner()
    {
        return requestedPlanner;
    }

    public String usedPlanner()
    {
        return usedPlanner;
    }

    public String defaultPlanner()
    {
        return defaultPlanner;
    }

    public String requestedRuntime()
    {
        return requestedRuntime;
    }

    public String usedRuntime()
    {
        return usedRuntime;
    }

    public String defaultRuntime()
    {
        return defaultRuntime;
    }

    public ExecutionPlanDescription planDescription()
    {
        return planDescription;
    }

    public Long dbHits()
    {
        return dbHits;
    }

    public Plan toPlan()
    {
        String version = (String) planDescription.getArguments().get( VERSION );
        return new Plan(
                requestedPlanner,
                usedPlanner,
                defaultPlanner,
                requestedRuntime,
                usedRuntime,
                defaultRuntime,
                version,
                new PlanTree(
                        asciiPlanDescription(),
                        toPlanOperator( planDescription ) )
        );
    }

    private static boolean isArgumentKey( String key )
    {
        return !NON_ARGUMENT_MAP_KEYS.contains( key );
    }

    public static PlanOperator toPlanOperator( ExecutionPlanDescription root )
    {
        Map<ExecutionPlanDescription,ExecutionPlanDescription> parentMap = new HashMap<>();
        Map<ExecutionPlanDescription,List<PlanOperator>> childrenMap = new HashMap<>();
        Stack<ExecutionPlanDescription> planDescriptionStack = new Stack<>();

        // initialization
        planDescriptionStack.push( root );
        childrenMap.put( root, new ArrayList<>() );

        while ( !planDescriptionStack.isEmpty() )
        {
            ExecutionPlanDescription planDescription = planDescriptionStack.pop();
            List<ExecutionPlanDescription> childrenDescriptions = planDescription.getChildren();
            List<PlanOperator> childOperators = childrenMap.get( planDescription );
            // this is a leaf plan _OR_ all leaves of this plan have already been processed
            if ( childrenDescriptions.isEmpty() || !childOperators.isEmpty() )
            {
                PlanOperator planOperator = toPlanOperator( planDescription, childOperators );
                ExecutionPlanDescription parentDescription = parentMap.get( planDescription );
                // if is not root
                if ( null != parentDescription )
                {
                    // add processed plan to children of parent
                    childrenMap.get( parentDescription ).add( planOperator );
                }
            }
            else
            {
                planDescriptionStack.push( planDescription );
                childrenDescriptions.forEach( child ->
                                              {
                                                  parentMap.put( child, planDescription );
                                                  childrenMap.put( child, new ArrayList<>() );
                                                  planDescriptionStack.push( child );
                                              } );
            }
        }

        return toPlanOperator( root, childrenMap.get( root ) );
    }

    private static PlanOperator toPlanOperator( ExecutionPlanDescription executionPlanDescription, List<PlanOperator> children )
    {
        String operatorType = executionPlanDescription.getName();
        Number estimatedRows = (Number) executionPlanDescription.getArguments().get( ESTIMATED_ROWS );
        ExecutionPlanDescription.ProfilerStatistics profilerStatistics = null;
        // some queries are run with 'explain' instead of 'profile', e.g., those with PERIODIC COMMIT
        if ( executionPlanDescription.hasProfilerStatistics() )
        {
            profilerStatistics = executionPlanDescription.getProfilerStatistics();
        }

        long dbHits = -1;
        if ( profilerStatistics != null && profilerStatistics.hasDbHits() )
        {
            dbHits = profilerStatistics.getDbHits();
        }

        long rows = -1;
        if ( profilerStatistics != null && profilerStatistics.hasRows() )
        {
            rows = profilerStatistics.getRows();
        }

        Map<String,String> arguments = executionPlanDescription.getArguments().entrySet().stream()
                                                               .filter( e -> isArgumentKey( e.getKey() ) )
                                                               .collect( toMap( Map.Entry::getKey, e -> e.getValue().toString() ) );

        List<String> identifiers = Lists.newArrayList( executionPlanDescription.getIdentifiers() );

        int id = idFor( executionPlanDescription );

        return new PlanOperator( id, operatorType, estimatedRows, dbHits, rows, arguments, identifiers, children );
    }

    public static int idFor( ExecutionPlanDescription planDescription )
    {
        if ( planDescription instanceof InternalPlanDescription )
        {
            return ((InternalPlanDescription) planDescription).id();
        }
        else
        {
            // Alternatively, this method could fallback to System::identityHashCode which is enough for our current tests to pass
            // System.identityHashCode( executionPlanDescription );
            throw new RuntimeException( "Plan description does not have an id\n" + planDescription );
        }
    }
}
