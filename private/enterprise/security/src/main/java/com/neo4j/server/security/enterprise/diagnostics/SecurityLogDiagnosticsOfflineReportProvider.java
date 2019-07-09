/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.diagnostics;

import com.neo4j.server.security.enterprise.configuration.SecuritySettings;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.diagnostics.DiagnosticsOfflineReportProvider;
import org.neo4j.kernel.diagnostics.DiagnosticsReportSource;

import static org.neo4j.kernel.diagnostics.DiagnosticsReportSources.newDiagnosticsRotatingFile;

@ServiceProvider
public class SecurityLogDiagnosticsOfflineReportProvider extends DiagnosticsOfflineReportProvider
{
    private FileSystemAbstraction fs;
    private Config config;

    public SecurityLogDiagnosticsOfflineReportProvider()
    {
        super( "logs" );
    }

    @Override
    public void init( FileSystemAbstraction fs, DatabaseIdRepository databaseIdRepository, Config config, File storeDirectory )
    {
        this.fs = fs;
        this.config = config;
    }

    @Override
    protected List<DiagnosticsReportSource> provideSources( Set<String> classifiers )
    {
        if ( classifiers.contains( "logs" ) )
        {
            File securityLog = config.get( SecuritySettings.security_log_filename );
            if ( fs.fileExists( securityLog ) )
            {
                return newDiagnosticsRotatingFile( "logs/security.log", fs, securityLog );
            }
        }
        return Collections.emptyList();
    }
}
