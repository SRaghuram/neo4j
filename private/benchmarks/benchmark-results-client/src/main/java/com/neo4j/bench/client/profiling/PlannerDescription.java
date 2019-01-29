package com.neo4j.bench.client.profiling;

import com.google.common.collect.Sets;
import com.neo4j.bench.client.model.Plan;
import com.neo4j.bench.client.model.PlanCompilationMetrics;
import com.neo4j.bench.client.model.PlanOperator;
import com.neo4j.bench.client.model.PlanTree;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Result;

import static java.util.stream.Collectors.toList;
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

    public static PlannerDescription fromResults( Result profileWithPlannerAndRuntime,
                                                  Result explainWithoutPlannerOrRuntime,
                                                  String requestedPlanner,
                                                  String requestedRuntime )
    {
        ExecutionPlanDescription usedExecutionPlanDescription = profileWithPlannerAndRuntime.getExecutionPlanDescription();
        Map<String,Object> usedArguments = usedExecutionPlanDescription.getArguments();
        String usedPlanner = (String) usedArguments.getOrDefault( PLANNER_IMPL, UNKNOWN );
        String usedRuntime = (String) usedArguments.getOrDefault( RUNTIME_IMPL, UNKNOWN );
        Long dbHits = (Long) usedArguments.get( DB_HITS );

        Map<String,Object> defaultArguments = explainWithoutPlannerOrRuntime.getExecutionPlanDescription().getArguments();
        String defaultPlanner = (String) defaultArguments.getOrDefault( PLANNER_IMPL, UNKNOWN );
        String defaultRuntime = (String) defaultArguments.getOrDefault( RUNTIME_IMPL, UNKNOWN );

        return new PlannerDescription( requestedPlanner.toLowerCase(),
                                       usedPlanner.toLowerCase(),
                                       defaultPlanner.toLowerCase(),
                                       requestedRuntime,
                                       usedRuntime.toLowerCase(),
                                       defaultRuntime.toLowerCase(),
                                       usedExecutionPlanDescription,
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

    public Plan toPlan( PlanCompilationMetrics compilationMetrics )
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
                compilationMetrics,
                new PlanTree(
                        asciiPlanDescription(),
                        toPlanOperator( planDescription ) )
        );
    }

    private static boolean isArgumentKey( String key )
    {
        return !NON_ARGUMENT_MAP_KEYS.contains( key );
    }

    private static PlanOperator toPlanOperator( ExecutionPlanDescription executionPlanDescription )
    {
        String name = executionPlanDescription.getName();
        Number estimatedRows = (Number) executionPlanDescription.getArguments().get( ESTIMATED_ROWS );
        // some queries are run with 'explain' instead of 'profile', e.g., those with PERIODIC COMMIT
        long dbHits = executionPlanDescription.hasProfilerStatistics()
                      ? executionPlanDescription.getProfilerStatistics().getDbHits()
                      : -1;
        // some queries are run with 'explain' instead of 'profile', e.g., those with PERIODIC COMMIT
        long rows = executionPlanDescription.hasProfilerStatistics()
                    ? executionPlanDescription.getProfilerStatistics().getRows()
                    : -1;
        Map<String,String> arguments = executionPlanDescription.getArguments().entrySet().stream()
                                                               .filter( e -> isArgumentKey( e.getKey() ) )
                                                               .collect( toMap( Map.Entry::getKey, e -> e.getValue().toString() ) );
        List<PlanOperator> children = executionPlanDescription.getChildren().stream()
                                                              .map( PlannerDescription::toPlanOperator )
                                                              .collect( toList() );
        return new PlanOperator( name, estimatedRows, dbHits, rows, arguments, children );
    }
}
