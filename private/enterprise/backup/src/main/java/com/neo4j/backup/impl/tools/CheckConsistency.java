/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl.tools;

import java.nio.file.Path;

import org.neo4j.configuration.Config;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

public class CheckConsistency implements BackupConsistencyChecker
{
    private final Config config;
    private final ConsistencyFlags consistencyFlags;
    private final Path reportDir;
    private final ProgressMonitorFactory progressMonitorFactory;
    private final FileSystemAbstraction fs;
    private final LogProvider logProvider;
    private final ConsistencyCheckService consistencyCheckService;

    public CheckConsistency( Config config, ConsistencyFlags consistencyFlags, Path reportDir, ProgressMonitorFactory progressMonitorFactory,
                             FileSystemAbstraction fs, LogProvider logProvider )
    {
        this.config = config;
        this.consistencyFlags = consistencyFlags;
        this.reportDir = reportDir;
        this.progressMonitorFactory = progressMonitorFactory;
        this.fs = fs;
        this.logProvider = logProvider;
        this.consistencyCheckService = new ConsistencyCheckService();
    }

    public void checkConsistency( DatabaseLayout databaseLayout ) throws ConsistencyCheckExecutionException
    {
        ConsistencyCheckService.Result ccResult;
        try
        {
            ccResult = consistencyCheckService.runFullConsistencyCheck(
                    databaseLayout,
                    Config.newBuilder().fromConfig( config ).build(),
                    progressMonitorFactory,
                    logProvider,
                    fs,
                    false,
                    reportDir,
                    consistencyFlags );
        }
        catch ( Exception e )
        {
            throw new ConsistencyCheckExecutionException( "Failed to do consistency check on the backup", e, true );
        }

        if ( !ccResult.isSuccessful() )
        {
            throw new ConsistencyCheckExecutionException( format( "Inconsistencies found on. See '%s' for details.", ccResult.reportFile() ), false );
        }
    }
}
