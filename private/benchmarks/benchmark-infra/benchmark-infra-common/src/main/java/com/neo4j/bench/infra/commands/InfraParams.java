/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.commands;

import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class InfraParams
{
    public static final String CMD_WORKER_ARTIFACT_URI = "--worker-artifact-uri";
    public static final String CMD_JOB_QUEUE = "--job-queue";
    public static final String CMD_JOB_DEFINITION = "--job-definition";

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

    // -----------------------------------------------------------------------
    // Common: Result Client Report Results Args
    // -----------------------------------------------------------------------

    public static final String CMD_RESULTS_STORE_USER = "--results_store_user";
    private final String resultsStoreUsername;

    public static final String CMD_RESULTS_STORE_PASSWORD = "--results_store_pass";
    private final String resultsStorePassword;

    public static final String CMD_RESULTS_STORE_URI = "--results_store_uri";

    public static final String CMD_BATCH_STACK = "--batch-stack";

    private final URI resultsStoreUri;

    public InfraParams( Path workspaceDir,
                        String awsSecret,
                        String awsKey,
                        String awsRegion,
                        String storeName,
                        String resultsStoreUsername,
                        String resultsStorePassword,
                        URI resultsStoreUri )
    {
        this.workspaceDir = workspaceDir;
        this.awsSecret = awsSecret;
        this.awsKey = awsKey;
        this.awsRegion = awsRegion;
        this.storeName = storeName;
        this.resultsStoreUsername = resultsStoreUsername;
        this.resultsStorePassword = resultsStorePassword;
        this.resultsStoreUri = resultsStoreUri;
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

    public Map<String,String> asMap()
    {
        Map<String,String> map = new HashMap<>();
        map.put( CMD_WORKSPACE_DIR, workspaceDir.toAbsolutePath().toString() );
        map.put( CMD_AWS_SECRET, awsSecret );
        map.put( CMD_AWS_KEY, awsKey );
        map.put( CMD_AWS_REGION, awsRegion );
        map.put( CMD_DB_NAME, storeName );
        return map;
    }
}
