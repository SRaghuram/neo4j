/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.commands;

import com.neo4j.bench.common.results.ErrorReportingPolicy;
import com.neo4j.bench.common.tool.macro.RunWorkloadParams;

import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class InfraParams
{
    public static final String CMD_JOB_QUEUE = "--job-queue";
    public static final String CMD_JOB_DEFINITION = "--job-definition";
    public static final String CMD_BATCH_STACK = "--batch-stack";

    public static final String CMD_WORKSPACE_DIR = "--workspace-dir";
    private final Path workspaceDir;

    public static final String CMD_AWS_SECRET = "--aws-secret";
    private final String awsSecret;

    public static final String CMD_AWS_KEY = "--aws-key";
    private final String awsKey;

    public static final String CMD_AWS_REGION = "--aws-region";
    private final String awsRegion;

    public static final String CMD_DB_NAME = "--db-name";
    private final String storeName;

    public static final String CMD_ARTIFACT_BASE_URI = "--artifact-base-uri";
    private final URI artifactBaseUri;

    public static final String CMD_ARTIFACT_WORKER_URI = "--worker-artifact-uri";
    private final URI artifactWorkerUri;

    // -----------------------------------------------------------------------
    // Common: Result Client Report Results Args
    // -----------------------------------------------------------------------

    public static final String CMD_RESULTS_STORE_USER = "--results_store_user";
    private final String resultsStoreUsername;

    public static final String CMD_RESULTS_STORE_PASSWORD = "--results_store_pass";
    private final String resultsStorePassword;

    public static final String CMD_RESULTS_STORE_URI = "--results_store_uri";
    private final URI resultsStoreUri;

    public static final String CMD_ERROR_POLICY = RunWorkloadParams.CMD_ERROR_POLICY;
    private ErrorReportingPolicy errorPolicy = ErrorReportingPolicy.REPORT_THEN_FAIL;

    public InfraParams( Path workspaceDir,
                        String awsSecret,
                        String awsKey,
                        String awsRegion,
                        String storeName,
                        String resultsStoreUsername,
                        String resultsStorePassword,
                        URI resultsStoreUri,
                        URI artifactBaseUri,
                        URI artifactWorkerUri,
                        ErrorReportingPolicy errorPolicy )
    {
        this.workspaceDir = workspaceDir;
        this.awsSecret = awsSecret;
        this.awsKey = awsKey;
        this.awsRegion = awsRegion;
        this.storeName = storeName;
        this.resultsStoreUsername = resultsStoreUsername;
        this.resultsStorePassword = resultsStorePassword;
        this.resultsStoreUri = resultsStoreUri;
        this.artifactBaseUri = artifactBaseUri;
        this.artifactWorkerUri = artifactWorkerUri;
        this.errorPolicy = errorPolicy;
    }

    public Path workspaceDir()
    {
        return workspaceDir;
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

    public String resultsStorePassword()
    {
        return resultsStorePassword;
    }

    public URI resultsStoreUri()
    {
        return resultsStoreUri;
    }

    public URI artifactBaseUri()
    {
        return artifactBaseUri;
    }

    public URI artifactWorkerUri()
    {
        return artifactWorkerUri;
    }

    public ErrorReportingPolicy errorReportingPolicy()
    {
        return errorPolicy;
    }

    public Map<String,String> asMap()
    {
        Map<String,String> map = new HashMap<>();
        map.put( CMD_AWS_SECRET, awsSecret );
        map.put( CMD_AWS_KEY, awsKey );
        map.put( CMD_AWS_REGION, awsRegion );
        map.put( CMD_DB_NAME, storeName );
        map.put( CMD_RESULTS_STORE_URI, resultsStoreUri.toString() );
        map.put( CMD_RESULTS_STORE_USER, resultsStoreUsername );
        map.put( CMD_ERROR_POLICY, String.valueOf( errorPolicy ) );
        return map;
    }
}
