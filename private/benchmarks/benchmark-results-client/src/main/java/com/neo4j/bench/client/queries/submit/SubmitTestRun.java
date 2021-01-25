/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries.submit;

import com.neo4j.bench.client.queries.Query;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.model.BenchmarkMetrics;
import com.neo4j.bench.model.model.Project;
import com.neo4j.bench.model.model.TestRun;
import com.neo4j.bench.model.model.TestRunReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;

import static com.neo4j.bench.model.model.BenchmarkMetrics.extractBenchmarkMetrics;
import static com.neo4j.bench.model.util.MapPrinter.prettyPrint;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.neo4j.driver.AccessMode.WRITE;

public class SubmitTestRun implements Query<SubmitTestRunResult>
{
    private static final Logger LOG = LoggerFactory.getLogger( SubmitTestRun.class );
    private static final String SUBMIT_TEST_RUN = Resources.fileToString( "/queries/write/submit_test_run.cypher" );

    private final TestRunReport report;
    private final Planner submitTreeWithPlanner;
    private String nonFatalError;

    public SubmitTestRun( TestRunReport report )
    {
        this( report, Planner.COST );
    }

    public SubmitTestRun( TestRunReport report, Planner submitTreeWithPlanner )
    {
        this.report = requireNonNull( report );
        this.submitTreeWithPlanner = submitTreeWithPlanner;
        this.nonFatalError = null;
    }

    @Override
    public SubmitTestRunResult execute( Driver driver )
    {
        LOG.debug( "submitting test results {}", report );

        try ( Session session = driver.session( SessionConfig.builder().withDefaultAccessMode( WRITE ).build() ) )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                Map<String,Object> params = params();
                Result statementResult = tx.run( SUBMIT_TEST_RUN, params );
                if ( statementResult.hasNext() )
                {
                    Record record = statementResult.next();

                    if ( statementResult.hasNext() )
                    {
                        throw new RuntimeException( "Query returned more than one row!" );
                    }

                    // benchmark_metrics : [[benchmark, metrics, params, [auxiliaryMetrics]]]
                    List<List<Value>> benchmarkMetricsLists = record.get( "benchmark_metrics" ).asList( e -> e.asList( v -> v ) );
                    List<BenchmarkMetrics> benchmarkMetrics =
                            benchmarkMetricsLists.stream()
                                                 .map( metrics ->
                                                       {
                                                           Map<String,Object> benchmarkMap = metrics.get( 0 ).asMap();
                                                           Map<String,Object> metricsMap = metrics.get( 1 ).asMap();
                                                           Map<String,Object> benchmarkParamsMap = metrics.get( 2 ).asMap();
                                                           /*
                                                           Note: at the moment submit_test_run.cypher supports creating more than one auxiliary metrics nodes,
                                                           but the rest of the stack restricts to [0,1] (i.e., one optional) nodes.
                                                           The following checks are just sanity checks to make sure that we really are only creating one.
                                                           If in future we want to create more than one auxiliary per metrics it would be trivial to support.
                                                            */
                                                           List<Map<String,Object>> auxiliaryMetricsMaps = metrics.get( 3 ).asList( Value::asMap );
                                                           if ( auxiliaryMetricsMaps.size() > 1 )
                                                           {
                                                               throw new RuntimeException( format( "Expected to create [0,1] auxiliary metrics but was %s",
                                                                                                   auxiliaryMetricsMaps.size() ) );
                                                           }
                                                           Map<String,Object> maybeAuxiliaryMetricsMap = auxiliaryMetricsMaps.isEmpty()
                                                                                                         ? null
                                                                                                         : auxiliaryMetricsMaps.get( 0 );
                                                           return extractBenchmarkMetrics( benchmarkMap,
                                                                                           metricsMap,
                                                                                           maybeAuxiliaryMetricsMap,
                                                                                           benchmarkParamsMap );
                                                       } )
                                                 .collect( toList() );
                    SubmitTestRunResult result = new SubmitTestRunResult(
                            new TestRun( record.get( "test_run" ).asMap() ),
                            benchmarkMetrics );

                    maybeSetNonFatalError( result.benchmarkMetricsList().size() );

                    if ( !report.benchmarkPlans().isEmpty() )
                    {
                        PlanTreeSubmitter.execute( tx, report.testRun(), report.benchmarkPlans(), submitTreeWithPlanner );
                    }
                    tx.commit();
                    return result;
                }
                else
                {
                    tx.rollback();
                    maybeSetNonFatalError( 0 );
                    return null;
                }
            }
        }
    }

    private void maybeSetNonFatalError( int actualResultSize )
    {
        if ( actualResultSize != report.benchmarkGroupBenchmarkMetrics().toList().size() )
        {
            nonFatalError = format( "Query created/returned unexpected number of metrics\n" +
                                    "Expected %s\n" +
                                    "Received %s",
                                    report.benchmarkGroupBenchmarkMetrics().toList().size(),
                                    actualResultSize );
        }
    }

    public Map<String,Object> params()
    {
        Map<String,Object> params = new HashMap<>();
        params.put( "metrics_tuples", report.benchmarkGroupBenchmarkMetrics().toList() );
        params.put( "tool_repository_name", report.benchmarkTool().repositoryName() );
        params.put( "tool_name", report.benchmarkTool().toolName() );
        params.put( "tool_commit", report.benchmarkTool().commit() );
        params.put( "tool_owner", report.benchmarkTool().owner() );
        params.put( "tool_branch", report.benchmarkTool().branch() );
        params.put( "jvm", report.java().jvm() );
        params.put( "jvm_version", report.java().version() );
        params.put( "jvm_args", report.java().jvmArgs() );
        params.put( "projects", report.projects().stream().map( Project::toMap ).collect( Collectors.toList() ) );
        params.put( "base_neo4j_config", report.baseNeo4jConfig().toMap() );
        params.put( "test_run", report.testRun().toMap() );
        params.put( "benchmark_config", report.benchmarkConfig().toMap() );
        params.put( "instances", report.environment().toMap() );
        return params;
    }

    @Override
    public Optional<String> nonFatalError()
    {
        return (null == nonFatalError) ? Optional.empty() : Optional.of( nonFatalError );
    }

    @Override
    public String toString()
    {
        return "Params:\n" +
               prettyPrint( params() ) +
               SUBMIT_TEST_RUN;
    }
}
