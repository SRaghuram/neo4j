/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries;

import com.neo4j.bench.common.model.BenchmarkMetrics;
import com.neo4j.bench.common.model.Project;
import com.neo4j.bench.common.model.TestRun;
import com.neo4j.bench.common.model.TestRunReport;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.util.Resources;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import static com.neo4j.bench.common.util.BenchmarkUtil.prettyPrint;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.neo4j.driver.v1.AccessMode.WRITE;

public class SubmitTestRun implements Query<SubmitTestRunResult>
{
    private static final String SUBMIT_TEST_RUN = Resources.fileToString( "/queries/write/submit_test_run.cypher" );

    private final TestRunReport report;
    private final Planner submitTreeWithPlanner;
    private String nonFatalError;

    public SubmitTestRun( TestRunReport report )
    {
        this( report, Planner.RULE );
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
        try ( Session session = driver.session( WRITE ) )
        {
            Map<String,Object> params = params();
            StatementResult statementResult = session.run( SUBMIT_TEST_RUN, params );

            if ( statementResult.hasNext() )
            {
                Record record = statementResult.next();

                if ( statementResult.hasNext() )
                {
                    throw new RuntimeException( "Query returned more than one row!" );
                }

                SubmitTestRunResult result = new SubmitTestRunResult(
                        new TestRun( record.get( "test_run" ).asMap() ),
                        extractBenchmarkMetricsList( record.get( "benchmark_metrics" ).asList() ) );

                maybeSetNonFatalError( result.benchmarkMetricsList().size() );

                if ( !report.benchmarkPlans().isEmpty() )
                {
                    PlanTreeSubmitter.execute( session, report.testRun(), report.benchmarkPlans(), submitTreeWithPlanner );
                }

                return result;
            }
            else
            {
                maybeSetNonFatalError( 0 );
                return null;
            }
        }
    }

    // [[benchmark,metrics,params]]
    private static List<BenchmarkMetrics> extractBenchmarkMetricsList( List<Object> benchmarkMetricsList )
    {
        return benchmarkMetricsList.stream()
                                   .map( o -> (List<Object>) o )
                                   .map( BenchmarkMetrics::extractBenchmarkMetrics )
                                   .collect( toList() );
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
        params.put( "operating_system", report.environment().operatingSystem() );
        params.put( "server", report.environment().server() );
        params.put( "jvm", report.java().jvm() );
        params.put( "jvm_version", report.java().version() );
        params.put( "jvm_args", report.java().jvmArgs() );
        params.put( "projects", report.projects().stream().map( Project::toMap ).collect( Collectors.toList() ) );
        params.put( "base_neo4j_config", report.baseNeo4jConfig().toMap() );
        params.put( "test_run", report.testRun().toMap() );
        params.put( "benchmark_config", report.benchmarkConfig().toMap() );
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
