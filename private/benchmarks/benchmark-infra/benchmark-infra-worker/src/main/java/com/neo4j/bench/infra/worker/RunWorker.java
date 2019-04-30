package com.neo4j.bench.infra.worker;
/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */

import com.amazonaws.SdkClientException;
import com.neo4j.bench.infra.Dataset;
import com.neo4j.bench.infra.InfraCommand;
import com.neo4j.bench.infra.Workspace;
import com.neo4j.bench.infra.aws.AWSS3ArtifactStorage;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.lang.String.join;

@Command( name = "run-worker" )
public class RunWorker extends InfraCommand
{

    private static final Logger LOG = LoggerFactory.getLogger( RunWorker.class );

    @Arguments
    private List<String> parameters;

    @Override
    public void run()
    {
        try
        {
            MacroBenchmarkRunArgs benchmarkArgs = MacroBenchmarkRunArgs.from(  parameters );
            String buildID = benchmarkArgs.getTeamcityBuild();
            String db  = benchmarkArgs.getDb();
            String neo4jBranch = benchmarkArgs.getNeo4jBranch();
            String neo4jVersion = benchmarkArgs.getNeo4jVersion();
            Path workspaceDir = Paths.get( workspacePath );

            AWSS3ArtifactStorage artifactStorage;
            if ( awsKey == null || awsSecret == null )
            {
                artifactStorage = AWSS3ArtifactStorage.create(awsRegion);
            }
            else
            {
                artifactStorage = AWSS3ArtifactStorage.create( awsRegion, awsKey, awsSecret );
            }
            artifactStorage.verifyBuildArtifactsExpirationRule();

            // download dataset
            Dataset dataset = artifactStorage.downloadDataset( neo4jBranch, db );
            Path runDir = workspaceDir.resolve( "macro" );
            dataset.extractInto( runDir );

            artifactStorage.downloadBuildArtifacts( workspaceDir, buildID );

            Workspace workspace = Workspace.create(
                    workspaceDir.toAbsolutePath(),
                    // required artifacts
                    Paths.get( "benchmark-infra-scheduler.jar" ),
                    Paths.get( format( "neo4j-%s-%s-unix.tar.gz", benchmarkArgs.getDbEdition().toLowerCase(), benchmarkArgs.getNeo4jVersion() ) ),
                    Paths.get( "macro/target/macro.jar" ),
                    Paths.get( "macro/run-report-benchmarks.sh" ) );

            Path neo4jConfig = runDir.resolve( "neo4j.conf" );
            workspace.extractNeo4jConfig( neo4jVersion, neo4jConfig );

            List<String> command = new ArrayList<>();
            Files.setPosixFilePermissions( runDir.resolve( "run-report-benchmarks.sh" ), PosixFilePermissions.fromString( "r-xr-xr-x" ) );
            command.add( "./run-report-benchmarks.sh" );
            command.addAll( benchmarkArgs.toArguments( neo4jConfig, runDir.resolve( "execute_work_dir" ) ) );

            LOG.info( "starting run report benchmark process, {}", join( " ", command ) );
            ProcessBuilder processBuilder = new ProcessBuilder( command )
                    .directory( runDir.toFile() )
                    .inheritIO();
//            processBuilder.environment().put( "JAVA_OPTS", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=6666" );
            Process process = processBuilder
                    .start();

            int waitFor = process.waitFor();
            if ( waitFor != 0 )
            {
                throw new RuntimeException( String.format( "benchmark exited with code %d", waitFor ) );
            }

        }
        catch ( SdkClientException | IOException | InterruptedException e )
        {
            LOG.error( "fatal error in worker", e );
        }

    }

}
