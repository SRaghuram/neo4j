/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.output;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;

import static com.neo4j.configuration.MetricsSettings.csv_interval;

public class CsvOutput implements Lifecycle
{
    private final Config config;
    private final Log logger;
    private final RotatableCsvReporter csvReporter;
    private final FileSystemAbstraction fileSystem;

    CsvOutput( Config config, Log logger, RotatableCsvReporter csvReporter, FileSystemAbstraction fileSystem )
    {
        this.config = config;
        this.logger = logger;
        this.csvReporter = csvReporter;
        this.fileSystem = fileSystem;
    }

    @Override
    public void init() throws IOException
    {
        // Ensure directory exists
        Path dir = csvReporter.outputPath();
        if ( !fileSystem.fileExists( dir ) )
        {
            fileSystem.mkdirs( dir );
        }
        if ( !fileSystem.isDirectory( dir ) )
        {
            throw new IllegalStateException( "The given path for CSV files points to a file, but a directory is required: " + dir.toAbsolutePath() );
        }
    }

    @Override
    public void start()
    {
        csvReporter.start( config.get( csv_interval ).toMillis(), TimeUnit.MILLISECONDS );
        logger.info( "Sending metrics to CSV file at " + csvReporter.outputPath() );
    }

    @Override
    public void stop()
    {
        csvReporter.stop();
    }

    @Override
    public void shutdown()
    {
    }
}
