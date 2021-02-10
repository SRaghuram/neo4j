/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.diagnostics;

import com.neo4j.causalclustering.core.consensus.log.segmented.FileNames;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.configuration.CausalClusteringSettings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.diagnostics.DiagnosticsOfflineReportProvider;
import org.neo4j.kernel.diagnostics.DiagnosticsReportSource;
import org.neo4j.kernel.diagnostics.DiagnosticsReportSources;
import org.neo4j.logging.NullLog;

import static com.neo4j.causalclustering.core.state.CoreStateFiles.RAFT_LOG;

@ServiceProvider
public class ClusterDiagnosticsOfflineReportProvider extends DiagnosticsOfflineReportProvider
{
    private FileSystemAbstraction fs;
    private ClusterStateLayout clusterStateLayout;
    private String defaultDatabaseName;

    public ClusterDiagnosticsOfflineReportProvider()
    {
        super( "raft", "ccstate" );
    }

    @Override
    public void init( FileSystemAbstraction fs, String defaultDatabaseName, Config config, Path ignoredStoreDir )
    {
        this.fs = fs;
        this.clusterStateLayout = ClusterStateLayout.of( config.get( CausalClusteringSettings.cluster_state_directory ) );
        this.defaultDatabaseName = defaultDatabaseName;
    }

    @Override
    protected List<DiagnosticsReportSource> provideSources( Set<String> classifiers )
    {
        List<DiagnosticsReportSource> sources = new ArrayList<>();
        if ( classifiers.contains( "raft" ) )
        {
            try
            {
                getRaftLogs( sources );
            }
            catch ( IOException e )
            {
                sources.add( DiagnosticsReportSources.newDiagnosticsString( "raft", e::getMessage ) );
            }
        }
        if ( classifiers.contains( "ccstate" ) )
        {
            getClusterState( sources );
        }

        return sources;
    }

    private void getRaftLogs( List<DiagnosticsReportSource> sources ) throws IOException
    {
        Path raftLogDir = clusterStateLayout.raftLogDirectory( defaultDatabaseName );
        boolean hasRaftLogDir = Files.exists( raftLogDir ) && Files.isDirectory( raftLogDir );
        if ( hasRaftLogDir )
        {
            FileNames fileNames = new FileNames( raftLogDir );
            SortedMap<Long,Path> allFiles = fileNames.getAllFiles( fs, NullLog.getInstance() );

            for ( Path logFile : allFiles.values() )
            {
                var destination = "raft" + raftLogDir.getFileSystem().getSeparator() + logFile.getFileName().toString();
                sources.add( DiagnosticsReportSources.newDiagnosticsFile( destination, fs, logFile ) );
            }
        }
    }

    private void getClusterState( List<DiagnosticsReportSource> sources )
    {
        clusterStateLayout.listGlobalAndDatabaseDirectories( defaultDatabaseName, type -> type != RAFT_LOG ).stream()
                   .filter( Files::exists )
                   .forEach( dir -> addDirectory( "ccstate", dir, sources ));
    }

    /**
     * Add all files in a directory recursively.
     *
     * @param path current relative path for destination.
     * @param dir current directory or file.
     * @param sources list of source that will be accumulated.
     */
    private void addDirectory( String path, Path dir, List<DiagnosticsReportSource> sources )
    {
        String currentLevel = path + dir.getFileSystem().getSeparator() + dir.getFileName();
        if ( fs.isDirectory( dir ) )
        {
            try
            {
                for ( Path file : fs.listFiles( dir ) )
                {
                    addDirectory( currentLevel, file, sources );
                }
            }
            catch ( IOException e )
            {
                sources.add( DiagnosticsReportSources
                        .newDiagnosticsString( "ccstate" + dir.getFileSystem().getSeparator() + dir.getFileName().toString(), e::getMessage ) );
            }
        }
        else // File
        {
            sources.add( DiagnosticsReportSources.newDiagnosticsFile( currentLevel, fs, dir ) );
        }
    }
}
