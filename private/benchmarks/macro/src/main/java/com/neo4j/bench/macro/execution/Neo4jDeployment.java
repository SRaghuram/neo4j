/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.neo4j.bench.model.options.Edition;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.macro.execution.process.DatabaseLauncher;

import java.time.Duration;

public abstract class Neo4jDeployment<DEPLOYMENT extends Deployment>
{
    public static Neo4jDeployment from( Deployment deployment )
    {
        if ( deployment instanceof Deployment.Embedded )
        {
            return new EmbeddedNeo4jDeployment( (Deployment.Embedded) deployment );
        }
        else if ( deployment instanceof Deployment.Server )
        {
            return new ServerNeo4jDeployment( (Deployment.Server) deployment );
        }
        else
        {
            throw new RuntimeException( "Invalid deployment mode value: " + deployment.getClass().getName() + " (" + deployment.toString() + ")" );
        }
    }

    private final DEPLOYMENT deployment;

    private Neo4jDeployment( DEPLOYMENT deployment )
    {
        this.deployment = deployment;
    }

    public final DEPLOYMENT deployment()
    {
        return deployment;
    }

    @Override
    public String toString()
    {
        return deployment().toString();
    }

    public abstract DatabaseLauncher<?> launcherFor( Edition edition,
                                                     int warmupCount,
                                                     int measurementCount,
                                                     Duration minMeasurementDuration,
                                                     Duration maxMeasurementDuration,
                                                     Jvm jvm );

    private static class EmbeddedNeo4jDeployment extends Neo4jDeployment<Deployment.Embedded>
    {
        private EmbeddedNeo4jDeployment( Deployment.Embedded deployment )
        {
            super( deployment );
        }

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
    }

    private static class ServerNeo4jDeployment extends Neo4jDeployment<Deployment.Server>
    {
        private ServerNeo4jDeployment( Deployment.Server deployment )
        {
            super( deployment );
        }

        @Override
        public DatabaseLauncher<?> launcherFor( Edition edition,
                                                int warmupCount,
                                                int measurementCount,
                                                Duration minMeasurementDuration,
                                                Duration maxMeasurementDuration,
                                                Jvm jvm )
        {
            return new DatabaseLauncher.ServerLauncher( deployment().path(),
                                                        warmupCount,
                                                        measurementCount,
                                                        minMeasurementDuration,
                                                        maxMeasurementDuration,
                                                        jvm );
        }
    }
}
