/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.commands;

import com.neo4j.bench.common.results.ErrorReportingPolicy;
import com.neo4j.bench.common.tool.macro.RunWorkloadParams;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.net.URI;

public class InfraParams
{
    public static final String CMD_JOB_QUEUE = "--job-queue";
    public static final String CMD_JOB_DEFINITION = "--job-definition";
    public static final String CMD_BATCH_STACK = "--batch-stack";
    public static final String CMD_WORKSPACE_DIR = "--workspace-dir";
    public static final String CMD_ARTIFACT_WORKER_URI = "--worker-artifact-uri";

    public static final String CMD_AWS_SECRET = "--aws-secret";
    private String awsSecret;

    public static final String CMD_AWS_KEY = "--aws-key";
    private String awsKey;

    public static final String CMD_AWS_REGION = "--aws-region";
    private String awsRegion;

    public static final String CMD_DB_NAME = "--db-name";
    private String storeName;

    public static final String CMD_ARTIFACT_BASE_URI = "--artifact-base-uri";
    private URI artifactBaseUri;

    // -----------------------------------------------------------------------
    // Common: Result Client Report Results Args
    // -----------------------------------------------------------------------

    public static final String CMD_RESULTS_STORE_USER = "--results-store-user";
    private String resultsStoreUsername;

    public static final String CMD_RESULTS_STORE_PASSWORD_SECRET_NAME = "--results-store-pass-secret-name";
    private String resultsStorePasswordSecretName;

    public static final String CMD_RESULTS_STORE_URI = "--results-store-uri";
    private URI resultsStoreUri;

    public static final String CMD_ERROR_POLICY = RunWorkloadParams.CMD_ERROR_POLICY;
    private ErrorReportingPolicy errorPolicy = ErrorReportingPolicy.REPORT_THEN_FAIL;

    // needed for JSON serialization
    private InfraParams()
    {
    }

    public InfraParams( String awsSecret,
                        String awsKey,
                        String awsRegion,
                        String storeName,
                        String resultsStoreUsername,
                        String resultsStorePasswordSecretName,
                        URI resultsStoreUri,
                        URI artifactBaseUri,
                        ErrorReportingPolicy errorPolicy )
    {
        this.awsSecret = awsSecret;
        this.awsKey = awsKey;
        this.awsRegion = awsRegion;
        this.storeName = storeName;
        this.resultsStoreUsername = resultsStoreUsername;
        this.resultsStorePasswordSecretName = resultsStorePasswordSecretName;
        this.resultsStoreUri = resultsStoreUri;
        this.artifactBaseUri = artifactBaseUri;
        this.errorPolicy = errorPolicy;
    }

    public String awsSecret()
    {
        return awsSecret;
    }

    public String awsKey()
    {
        return awsKey;
    }

    public boolean hasAwsCredentials()
    {
        return awsSecret != null && awsKey != null;
    }

    public String awsRegion()
    {
        return awsRegion;
    }

    public String storeName()
    {
        return storeName;
    }

    public String resultsStoreUsername()
    {
        return resultsStoreUsername;
    }

    public String resultsStorePasswordSecretName()
    {
        return resultsStorePasswordSecretName;
    }

    public URI resultsStoreUri()
    {
        return resultsStoreUri;
    }

    public URI artifactBaseUri()
    {
        return artifactBaseUri;
    }

    public ErrorReportingPolicy errorReportingPolicy()
    {
        return errorPolicy;
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
        InfraParams that = (InfraParams) o;
        return EqualsBuilder.reflectionEquals( this, that );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }
}
