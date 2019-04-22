/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.neo4j.bench.client.model.Edition;
import com.neo4j.bench.client.util.BenchmarkUtil;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.macro.execution.process.DatabaseLauncher;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;

import static com.neo4j.bench.macro.execution.Neo4jDeployment.DeploymentMode.EMBEDDED;
import static com.neo4j.bench.macro.execution.Neo4jDeployment.DeploymentMode.SERVER;

public abstract class Neo4jDeployment
{
    public enum DeploymentMode
    {
        EMBEDDED,
        SERVER
    }

    private static final String SERVER_PREFIX = SERVER.name() + ":";

    public static Neo4jDeployment parse( String value )
    {
        if ( value.equalsIgnoreCase( EMBEDDED.name() ) )
        {
            return new EmbeddedNeo4jDeployment();
        }
        else if ( value.toUpperCase().startsWith( SERVER_PREFIX ) )
        {
            String neo4jDirString = value.substring( SERVER_PREFIX.length() );
            Path neo4jDir = Paths.get( neo4jDirString );
            BenchmarkUtil.assertDirectoryExists( neo4jDir );
            return new ServerNeo4jDeployment( neo4jDir );
        }
        else
        {
            throw new RuntimeException( "Invalid deployment mode value: '" + value + "'\n" +
                                        "Expected one of: " + Arrays.toString( DeploymentMode.values() ) );
        }
    }

    public static Neo4jDeployment server( Path neo4jDir )
    {
        BenchmarkUtil.assertDirectoryExists( neo4jDir );
        return new ServerNeo4jDeployment( neo4jDir );
    }

    public static Neo4jDeployment embedded()
    {
        return new EmbeddedNeo4jDeployment();
    }

    public abstract DatabaseLauncher<?> launcherFor( Edition edition,
                                                     int warmupCount,
                                                     int measurementCount,
                                                     Duration minMeasurementDuration,
                                                     Duration maxMeasurementDuration,
                                                     Jvm jvm );

    public abstract DeploymentMode mode();

    private static class EmbeddedNeo4jDeployment extends Neo4jDeployment
    {
        @Override
        public DatabaseLauncher<?> launcherFor( Edition edition,
                                                int warmupCount,
                                                int measurementCount,
                                                Duration minMeasurementDuration,
                                                Duration maxMeasurementDuration,
                                                Jvm jvm )
        {
            return new DatabaseLauncher.EmbeddedLauncher( edition,
                                                          warmupCount,
                                                          measurementCount,
                                                          minMeasurementDuration,
                                                          maxMeasurementDuration,
                                                          jvm );
        }

        @Override
        public DeploymentMode mode()
        {
            return EMBEDDED;
        }

        @Override
        public String toString()
        {
            return EMBEDDED.name();
        }
    }

    private static class ServerNeo4jDeployment extends Neo4jDeployment
    {
        private final Path neo4jDir;

        private ServerNeo4jDeployment( Path neo4jDir )
        {
            this.neo4jDir = neo4jDir;
        }

        @Override
        public DatabaseLauncher<?> launcherFor( Edition edition,
                                                int warmupCount,
                                                int measurementCount,
                                                Duration minMeasurementDuration,
                                                Duration maxMeasurementDuration,
                                                Jvm jvm )
        {
            return new DatabaseLauncher.ServerLauncher( neo4jDir,
                                                        warmupCount,
                                                        measurementCount,
                                                        minMeasurementDuration,
                                                        maxMeasurementDuration,
                                                        jvm );
        }

        @Override
        public DeploymentMode mode()
        {
            return SERVER;
        }

        @Override
        public String toString()
        {
            return SERVER_PREFIX + neo4jDir.toAbsolutePath().toString();
        }
    }
}
