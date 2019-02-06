/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries;

import com.neo4j.bench.client.model.Project;
import com.neo4j.bench.client.model.TestRun;
import com.neo4j.bench.client.model.TestRunReport;
import com.neo4j.bench.client.options.Planner;
import com.neo4j.bench.client.util.Resources;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import static com.neo4j.bench.client.ClientUtil.prettyPrint;
import static com.neo4j.bench.client.model.BenchmarkMetrics.extractBenchmarkMetricsList;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

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
        SubmitTestRunResult result;
        try ( Session session = driver.session() )
        {
            Map<String,Object> params = params();
            StatementResult statementResult = session.run( SUBMIT_TEST_RUN, params );
            Record record = statementResult.single();
            result = new SubmitTestRunResult(
                    new TestRun( record.get( "test_run" ) ),
                    extractBenchmarkMetricsList( record.get( "benchmark_metrics" ) ) );
            if ( result.benchmarkMetricsList().size() != report.benchmarkGroupBenchmarkMetrics().toList().size() )
            {
                nonFatalError = format( "Query created/returned unexpected number of metrics\n" +
                                        "Expected %s\n" +
                                        "Received %s",
                                        report.benchmarkGroupBenchmarkMetrics().toList().size(),
                                        result.benchmarkMetricsList().size() );
            }

            if ( !report.benchmarkPlans().isEmpty() )
            {
                PlanTreeSubmitter.execute( session, report.testRun(), report.benchmarkPlans(), submitTreeWithPlanner );
            }
        }

        return result;
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
               prettyPrint( params(), "\t" ) +
               SUBMIT_TEST_RUN;
    }
}
