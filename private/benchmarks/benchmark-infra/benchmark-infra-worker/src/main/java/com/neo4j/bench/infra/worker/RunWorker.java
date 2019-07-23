package com.neo4j.bench.infra.worker;
/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */

import com.amazonaws.SdkClientException;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.neo4j.bench.infra.ArtifactStoreException;
import com.neo4j.bench.infra.Dataset;
import com.neo4j.bench.infra.InfraCommand;
import com.neo4j.bench.infra.Workspace;
import com.neo4j.bench.infra.aws.AWSS3ArtifactStorage;
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
            Path runDir = workspaceDir.resolve( "macro" );

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

            // download & extract dataset
            Dataset dataset = artifactStorage.downloadDataset( neo4jBranch, db );
            dataset.extractInto( runDir );

            // download artifacts
            artifactStorage.downloadBuildArtifacts( workspaceDir, buildID );
            Files.setPosixFilePermissions( runDir.resolve( "run-report-benchmarks.sh" ), PosixFilePermissions.fromString( "r-xr-xr-x" ) );

            Workspace workspace = Workspace
                        .create( workspaceDir.toAbsolutePath() )
                        .withArtifacts(
                            // required artifacts
                            Paths.get( "benchmark-infra-scheduler.jar" ),
                            Paths.get( format( "neo4j-%s-%s-unix.tar.gz", benchmarkArgs.getDbEdition().toLowerCase(), benchmarkArgs.getNeo4jVersion() ) ),
                            Paths.get( "macro/target/macro.jar" ),
                            Paths.get( "macro/run-report-benchmarks.sh" )
                        ).build();

            // extract neo4j config
            Path neo4jConfig = runDir.resolve( "neo4j.conf" );
            workspace.extractNeo4jConfig( neo4jVersion, neo4jConfig );

            // prepare command line to invoke run-report-benchmarks.sh
            List<String> command = new ArrayList<>();
            command.add( "./run-report-benchmarks.sh" );
            Path executeWorkDir = runDir.resolve( "execute_work_dir" );
            Files.createDirectories( executeWorkDir );
            command.addAll( benchmarkArgs.toArguments( neo4jConfig, executeWorkDir ) );

            LOG.info( "starting run report benchmark process, {}", join( " ", command ) );
            Process process = new ProcessBuilder( command )
                    .directory( runDir.toFile() )
                    .inheritIO()
                    .start();

            int waitFor = process.waitFor();
            if ( waitFor != 0 )
            {
                throw new RuntimeException( format( "benchmark exited with code %d", waitFor ) );
            }

        }
        catch ( SdkClientException | IOException | InterruptedException | ArtifactStoreException e )
        {
            throw new RuntimeException( "fatal error in worker", e );
        }

    }

}
