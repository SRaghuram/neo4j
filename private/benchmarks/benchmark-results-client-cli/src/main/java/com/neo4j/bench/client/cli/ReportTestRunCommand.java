/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.cli;

import com.github.rvesse.airline.annotations.Command;
import com.neo4j.bench.client.reporter.ResultsReporter;
import com.neo4j.bench.common.command.ReportTestRunArgs;
import com.neo4j.bench.common.command.ResultsStoreArgs;
import com.neo4j.bench.model.model.TestRunReport;
import com.neo4j.bench.model.util.JsonUtil;

import javax.inject.Inject;

@Command( name = "report-test-run" )
public class ReportTestRunCommand implements Runnable
{

    @Inject
    private final ResultsStoreArgs resultsStoreArgs = new ResultsStoreArgs();

    @Inject
    private final ReportTestRunArgs reportTestRunArgs = new ReportTestRunArgs();

    @Override
    public void run()
    {
        TestRunReport testRunReport = JsonUtil.deserializeJson( reportTestRunArgs.testRunReportFile().toPath(), TestRunReport.class );
        ResultsReporter resultsReporter = new ResultsReporter( resultsStoreArgs.resultsStoreUsername(),
                                                               resultsStoreArgs.resultsStorePassword(),
                                                               resultsStoreArgs.resultsStoreUri() );
        resultsReporter.reportAndUpload( testRunReport,
                                         reportTestRunArgs.recordingsBaseUri(),
                                         reportTestRunArgs.workDir(),
                                         reportTestRunArgs.awsEndpointURL(),
                                         reportTestRunArgs.errorReportingPolicy() );
    }
}
