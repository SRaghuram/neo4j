/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.command;

import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.neo4j.bench.common.results.ErrorReportingPolicy;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.File;
import java.net.URI;

public class ReportTestRunArgs
{

    private static final String CMD_TEST_RUN_REPORT_FILE = "--test-run-report";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TEST_RUN_REPORT_FILE},
            description = "Test run report JSON file",
            title = "Test run report" )
    @Required
    private File testRunReportFile;

    private static final String CMD_RECORDINGS_BASE_URI = "--recordings-base-uri";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RECORDINGS_BASE_URI},
            description = "S3 bucket recorsings and profiles were uploaded to",
            title = "Recordings and profiles S3 URI" )
    private URI recordingsBaseUri;

    private static final String CMD_WORK_DIR = "--working-dir";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WORK_DIR},
            description = "Working directory where we will lookup up for profiler recordings",
            title = "Working Directory" )
    @Required
    private File workDir;

    private static final String CMD_AWS_ENDPOINT_URL = "--aws-endpoint-url";
    @Option( type = OptionType.COMMAND,
            name = {CMD_AWS_ENDPOINT_URL},
            description = "AWS endpoint URL, used during testing",
            title = "AWS endpoint URL" )
    private String awsEndpointURL;

    private static final String CMD_ERROR_REPORTING_POLICY = "--error-reporting";
    @Option( type = OptionType.COMMAND,
            name = {CMD_ERROR_REPORTING_POLICY},
            description = "Error reporting policy",
            title = "Error reporting policy" )
    private ErrorReportingPolicy errorReportingPolicy = ErrorReportingPolicy.REPORT_THEN_FAIL;

    public File testRunReportFile()
    {
        return testRunReportFile;
    }

    public URI recordingsBaseUri()
    {
        return recordingsBaseUri;
    }

    public File workDir()
    {
        return workDir;
    }

    public String awsEndpointURL()
    {
        return awsEndpointURL;
    }

    public ErrorReportingPolicy errorReportingPolicy()
    {
        return errorReportingPolicy;
    }

    @Override
    public boolean equals( Object o )
    {

        return EqualsBuilder.reflectionEquals( this, o );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString( this, ToStringStyle.SHORT_PREFIX_STYLE );
    }
}
