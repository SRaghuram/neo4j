/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.neo4j.bench.common.results.ErrorReportingPolicy;
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
    public static final String CMD_AWS_KEY = "--aws-key";
    public static final String CMD_AWS_REGION = "--aws-region";

    public static final String CMD_ARTIFACT_BASE_URI = "--artifact-base-uri";
    public static final String CMD_AWS_ENDPOINT_URL = "--aws-endpoint-url";
    private final URI artifactBaseUri;

    private final Workspace workspaceStructure;

    // -----------------------------------------------------------------------
    // Common: Result Client Report Results Args
    // -----------------------------------------------------------------------

    public static final String CMD_RESULTS_STORE_USER = "--results-store-user";
    private final String resultsStoreUsername;

    public static final String CMD_RESULTS_STORE_PASSWORD_SECRET_NAME = "--results-store-pass-secret-name";
    private final String resultsStorePasswordSecretName;

    public static final String CMD_RESULTS_STORE_URI = "--results-store-uri";
    private final URI resultsStoreUri;

    private final ErrorReportingPolicy errorPolicy;

    private final AWSCredentials awsCredentials;

    public static final String CMD_RESULTS_STORE_PASSWORD = "--results-store-pass";
    private final String resultsStorePassword;

    @Deprecated
    public InfraParams( AWSCredentials awsCredentials,
                        String resultsStoreUsername,
                        String resultsStorePasswordSecretName,
                        URI resultsStoreUri,
                        URI artifactBaseUri,
                        ErrorReportingPolicy errorPolicy,
                        Workspace workspaceStructure )
    {
        this( awsCredentials,
              resultsStoreUsername,
              resultsStorePasswordSecretName,
              null,
              resultsStoreUri,
              artifactBaseUri,
              errorPolicy,
              workspaceStructure );
    }

    @JsonCreator
    public InfraParams( @JsonProperty( "awsCredentials" ) AWSCredentials awsCredentials,
                        @JsonProperty( "resultsStoreUsername" ) String resultsStoreUsername,
                        @JsonProperty( "resultsStorePasswordSecretName" ) String resultsStorePasswordSecretName,
                        @JsonProperty( "resultsStorePassword" ) String resultsStorePassword,
                        @JsonProperty( "resultsStoreUri" ) URI resultsStoreUri,
                        @JsonProperty( "artifactBaseUri" ) URI artifactBaseUri,
                        @JsonProperty( "errorPolicy" ) ErrorReportingPolicy errorPolicy,
                        @JsonProperty( "workspaceStructure" ) Workspace workspaceStructure )
    {
        this.awsCredentials = awsCredentials;
        this.resultsStoreUsername = resultsStoreUsername;
        this.resultsStorePasswordSecretName = resultsStorePasswordSecretName;
        this.resultsStorePassword = resultsStorePassword;
        this.resultsStoreUri = resultsStoreUri;
        this.artifactBaseUri = artifactBaseUri;
        this.errorPolicy = errorPolicy;
        this.workspaceStructure = workspaceStructure;
    }

    public AWSCredentials awsCredentials()
    {
        return awsCredentials;
    }

    public String resultsStoreUsername()
    {
        return resultsStoreUsername;
    }

    public String resultsStorePasswordSecretName()
    {
        return resultsStorePasswordSecretName;
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

    public ErrorReportingPolicy errorReportingPolicy()
    {
        return errorPolicy;
    }

    public Workspace workspaceStructure()
    {
        return workspaceStructure;
    }

    public InfraParams withArtifactBaseUri( URI newArtifactBaseUri )
    {
        return new InfraParams( awsCredentials,
                                resultsStoreUsername,
                                resultsStorePasswordSecretName,
                                resultsStorePassword,
                                resultsStoreUri,
                                newArtifactBaseUri,
                                errorPolicy,
                                workspaceStructure );
    }

    @Override
    public boolean equals( Object that )
    {
        return EqualsBuilder.reflectionEquals( this, that );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }
}
