/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.neo4j.bench.common.database.Neo4jStore;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.macro.execution.process.DatabaseLauncher;
import com.neo4j.bench.macro.execution.process.MeasurementOptions;
import com.neo4j.bench.model.options.Edition;

import java.nio.file.Path;

public abstract class Neo4jDeployment<DEPLOYMENT extends Deployment>
{
    public static Neo4jDeployment from( Deployment deployment,
                                        Edition edition,
                                        MeasurementOptions measurementOptions,
                                        Jvm jvm,
                                        Path storeDir )
    {
        if ( deployment instanceof Deployment.Embedded )
        {
            return new EmbeddedNeo4jDeployment( (Deployment.Embedded) deployment, edition, measurementOptions, jvm, Neo4jStore.createFrom( storeDir ) );
        }
        else if ( deployment instanceof Deployment.Server )
        {
            return new ServerNeo4jDeployment( (Deployment.Server) deployment, measurementOptions, jvm, Neo4jStore.createFrom( storeDir ) );
        }
        else
        {
            throw new RuntimeException( "Invalid deployment mode value: " + deployment.getClass().getName() + " (" + deployment.toString() + ")" );
        }
    }

    private final DEPLOYMENT deployment;
    protected final MeasurementOptions measurementOptions;
    protected final Jvm jvm;
    protected final Store originalStore;

    private Neo4jDeployment( DEPLOYMENT deployment, MeasurementOptions measurementOptions, Jvm jvm, Store originalStore )
    {
        this.deployment = deployment;
        this.measurementOptions = measurementOptions;
        this.jvm = jvm;
        this.originalStore = originalStore;
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

    public abstract DatabaseLauncher<?> launcherFor( boolean copyStore, Path neo4jConfigFile, ForkDirectory forkDirectory );

    private static class EmbeddedNeo4jDeployment extends Neo4jDeployment<Deployment.Embedded>
    {
        private final Edition edition;

        private EmbeddedNeo4jDeployment( Deployment.Embedded deployment, Edition edition, MeasurementOptions measurementOptions, Jvm jvm, Store originalStore )
        {
            super( deployment, measurementOptions, jvm, originalStore );
            this.edition = edition;
        }

        @Override
        public DatabaseLauncher<?> launcherFor( boolean copyStore, Path neo4jConfigFile, ForkDirectory forkDirectory )
        {
            return new DatabaseLauncher.EmbeddedLauncher( edition,
                                                          measurementOptions,
                                                          jvm,
                                                          originalStore,
                                                          copyStore,
                                                          neo4jConfigFile,
                                                          forkDirectory );
        }
    }

    private static class ServerNeo4jDeployment extends Neo4jDeployment<Deployment.Server>
    {
        private ServerNeo4jDeployment( Deployment.Server deployment, MeasurementOptions measurementOptions, Jvm jvm, Store originalStore )
        {
            super( deployment, measurementOptions, jvm, originalStore );
        }

        @Override
        public DatabaseLauncher<?> launcherFor( boolean copyStore, Path neo4jConfigFile, ForkDirectory forkDirectory )
        {
            return new DatabaseLauncher.ServerLauncher( deployment().path(),
                                                        measurementOptions,
                                                        jvm,
                                                        originalStore,
                                                        copyStore,
                                                        neo4jConfigFile,
                                                        forkDirectory );
        }
    }
}
