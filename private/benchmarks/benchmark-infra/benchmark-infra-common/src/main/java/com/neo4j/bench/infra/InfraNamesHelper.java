/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import org.apache.commons.lang3.StringUtils;

import static java.util.Objects.requireNonNull;

public final class InfraNamesHelper
{
    public static String sanitizeJobName( String jobName )
    {
        return sanitizeInfraName( jobName );
    }

    public static String sanitizeJobDefinitionName( String jobDefinitionName )
    {
        return sanitizeInfraName( jobDefinitionName );
    }

    public static String sanitizeDockerImageTag( String tag )
    {
        return StringUtils.substring(
                requireNonNull( tag, "docker image tag name cannot be null" ).replaceAll( "[^\\p{Alnum}|^_]", "_" ),
                0,
                127 );
    }

    public static String sanitizeJobQueueName( String jobQueueName )
    {
        return sanitizeInfraName( jobQueueName );
    }

    public static String sanitizeComputeEnvironmentName( String computeEnvironmentName )
    {
        return sanitizeInfraName( computeEnvironmentName );
    }

    /**
     * Job name should follow these restrictions, https://docs.aws.amazon.com/cli/latest/reference/batch/submit-job.html The first character must be
     * alphanumeric, and up to 128 letters (uppercase and lowercase), numbers, hyphens, and underscores are allowed.
     */
    private static String sanitizeInfraName( String name )
    {
        return StringUtils.substring(
                requireNonNull( name, "name cannot be null" ).replaceAll( "[^\\p{Alnum}|^_-]", "_" ), 0, 127 );
    }

    private InfraNamesHelper()
    {
        // no op
    }
}
