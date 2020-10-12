/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.query;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.diagnostics.DiagnosticsOfflineReportProvider;
import org.neo4j.kernel.diagnostics.DiagnosticsReportSource;

import static org.neo4j.kernel.diagnostics.DiagnosticsReportSources.newDiagnosticsRotatingFile;

@ServiceProvider
public class QueryLoggerDiagnosticsOfflineReportProvider extends DiagnosticsOfflineReportProvider
{
    private FileSystemAbstraction fs;
    private Config config;

    public QueryLoggerDiagnosticsOfflineReportProvider()
    {
        super( "logs" );
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
        if ( classifiers.contains( "logs" ) )
        {
            Path queryLog = config.get( GraphDatabaseSettings.log_queries_filename );
            if ( fs.fileExists( queryLog ) )
            {
                return newDiagnosticsRotatingFile( "logs/", fs, queryLog );
            }
        }
        return Collections.emptyList();
    }
}
