/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.output;

import com.codahale.metrics.MetricRegistry;
import com.neo4j.configuration.MetricsSettings;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.MonitoredJobExecutor;

import static com.neo4j.configuration.MetricsSettings.csv_enabled;
import static com.neo4j.configuration.MetricsSettings.csv_interval;
import static com.neo4j.configuration.MetricsSettings.csv_path;
import static org.neo4j.scheduler.JobMonitoringParams.systemJob;

public class CsvOutput implements Lifecycle
{
    private final Config config;
    private final MetricRegistry registry;
    private final Log logger;
    private final ExtensionContext extensionContext;
    private final FileSystemAbstraction fileSystem;
    private final JobScheduler scheduler;
    private RotatableCsvReporter csvReporter;
    private Path outputPath;

    CsvOutput( Config config, MetricRegistry registry, Log logger, ExtensionContext extensionContext,
            FileSystemAbstraction fileSystem, JobScheduler scheduler )
    {
        this.config = config;
        this.registry = registry;
        this.logger = logger;
        this.extensionContext = extensionContext;
        this.fileSystem = fileSystem;
        this.scheduler = scheduler;
    }

    @Override
    public void init() throws IOException
    {
        // Setup CSV reporting
        Path configuredPath = config.get( csv_path );
        if ( configuredPath == null )
        {
            throw new IllegalArgumentException( csv_path.name() + " configuration is required since " +
                                                csv_enabled.name() + " is enabled" );
        }
        Long rotationThreshold = config.get( MetricsSettings.csv_rotation_threshold );
        Integer maxArchives = config.get( MetricsSettings.csv_max_archives );
        outputPath = absoluteFileOrRelativeTo( extensionContext.directory(), configuredPath );
        csvReporter = RotatableCsvReporter.forRegistry( registry )
                .convertRatesTo( TimeUnit.SECONDS )
                .convertDurationsTo( TimeUnit.MILLISECONDS )
                .formatFor( Locale.US )
                .outputStreamSupplierFactory(
                        getFileRotatingFileOutputStreamSupplier( rotationThreshold, maxArchives ) )
                .build( ensureDirectoryExists( outputPath ) );
    }

    @Override
    public void start()
    {
        csvReporter.start( config.get( csv_interval ).toMillis(), TimeUnit.MILLISECONDS );
        logger.info( "Sending metrics to CSV file at " + outputPath );
    }

    @Override
    public void stop()
    {
        csvReporter.stop();
    }

    @Override
    public void shutdown()
    {
        csvReporter = null;
    }

    private BiFunction<Path,RotatingFileOutputStreamSupplier.RotationListener,RotatingFileOutputStreamSupplier> getFileRotatingFileOutputStreamSupplier(
            Long rotationThreshold, Integer maxArchives )
    {
        return ( file, listener ) -> {
            try
            {
                MonitoredJobExecutor monitoredJobExecutor = scheduler.monitoredJobExecutor( Group.LOG_ROTATION );
                Executor executor = job -> monitoredJobExecutor.execute( systemJob( "Rotation of CSV metrics output file '" + file.getFileName() + "'" ), job );
                return new RotatingFileOutputStreamSupplier( fileSystem, file, rotationThreshold, 0, maxArchives, executor, listener );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        };
    }

    private Path ensureDirectoryExists( Path dir ) throws IOException
    {
        if ( !fileSystem.fileExists( dir ) )
        {
            fileSystem.mkdirs( dir );
        }
        if ( !fileSystem.isDirectory( dir ) )
        {
            throw new IllegalStateException(
                    "The given path for CSV files points to a file, but a directory is required: " +
                            dir.toAbsolutePath() );
        }
        return dir;
    }

    /**
     * Looks at configured file {@code absoluteOrRelativeFile} and just returns it if absolute, otherwise
     * returns a {@link Path} with {@code baseDirectoryIfRelative} as parent.
     *
     * @param baseDirectoryIfRelative base directory to use as parent if {@code absoluteOrRelativeFile}
     * is relative, otherwise unused.
     * @param absoluteOrRelativeFile file to return as absolute or relative to {@code baseDirectoryIfRelative}.
     */
    private static Path absoluteFileOrRelativeTo( Path baseDirectoryIfRelative, Path absoluteOrRelativeFile )
    {
        return absoluteOrRelativeFile.isAbsolute()
                ? absoluteOrRelativeFile
                : baseDirectoryIfRelative.resolve( absoluteOrRelativeFile );
    }
}
