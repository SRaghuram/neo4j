/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.diagnostics;

import com.neo4j.configuration.MetricsSettings;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.diagnostics.DiagnosticsOfflineReportProvider;
import org.neo4j.kernel.diagnostics.DiagnosticsReportSource;

import static org.neo4j.kernel.diagnostics.DiagnosticsReportSources.newDiagnosticsFile;

@ServiceProvider
public class MetricsDiagnosticsOfflineReportProvider extends DiagnosticsOfflineReportProvider
{
    private FileSystemAbstraction fs;
    private Config config;

    public MetricsDiagnosticsOfflineReportProvider()
    {
        super( "metrics" );
    }

    @Override
    public void init( FileSystemAbstraction fs, String defaultDatabaseName, Config config, Path storeDirectory )
    {
        this.fs = fs;
        this.config = config;
    }

    @Override
    protected List<DiagnosticsReportSource> provideSources( Set<String> classifiers )
    {
        Path metricsDirectory = config.get( MetricsSettings.csv_path );
        if ( fs.fileExists( metricsDirectory.toFile() ) && fs.isDirectory( metricsDirectory.toFile() ) )
        {
            List<DiagnosticsReportSource> files = new ArrayList<>();
            for ( File file : fs.listFiles( metricsDirectory.toFile() ) )
            {
                files.add( newDiagnosticsFile( "metrics/" + file.getName(), fs, file.toPath() ) );
            }
            return files;
        }
        return Collections.emptyList();
    }
}
