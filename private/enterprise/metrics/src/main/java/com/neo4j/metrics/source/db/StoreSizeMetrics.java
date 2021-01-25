/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.neo4j.metrics.metric.MetricsRegister;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.scheduler.Group.FILE_IO_HELPER;

@Documented( ".Database store size metrics" )
public class StoreSizeMetrics extends LifecycleAdapter
{
    public static final Duration UPDATE_INTERVAL = Duration.ofMinutes( 10 );

    private static final String PREFIX = "store.size";

    @Documented( "The total size of the database and transaction logs, in bytes. (gauge)" )
    private static final String TOTAL_STORE_SIZE = name( PREFIX, "total" );
    @Documented( "The size of the database, in bytes. (gauge)" )
    private static final String DATABASE_SIZE = name( PREFIX, "database" );

    private final MetricsRegister registry;
    private final String totalStoreSize;
    private final String databaseSize;
    private final JobScheduler scheduler;
    private final FileSystemAbstraction fileSystem;
    private final DatabaseLayout databaseLayout;

    private volatile JobHandle updateValuesHandle;
    private volatile long cachedStoreTotalSize = -1L;
    private volatile long cachedDatabaseSize = -1L;

    public StoreSizeMetrics( String metricsPrefix, MetricsRegister registry, JobScheduler scheduler, FileSystemAbstraction fileSystemAbstraction,
            DatabaseLayout databaseLayout )
    {
        this.registry = registry;
        this.totalStoreSize = name( metricsPrefix, TOTAL_STORE_SIZE );
        this.databaseSize = name( metricsPrefix, DATABASE_SIZE );
        this.scheduler = scheduler;
        this.fileSystem = fileSystemAbstraction;
        this.databaseLayout = databaseLayout;
    }

    @Override
    public void start()
    {
        if ( updateValuesHandle == null )
        {
            var monitoringParams = JobMonitoringParams.systemJob( databaseLayout.getDatabaseName(), "Update of database store metrics" );
            updateValuesHandle =
                    scheduler.scheduleRecurring( FILE_IO_HELPER, monitoringParams, this::updateCachedValues, UPDATE_INTERVAL.toMillis(), MILLISECONDS );
        }
        registry.register( totalStoreSize, () -> (Gauge<Long>) () -> cachedStoreTotalSize );
        registry.register( databaseSize, () -> (Gauge<Long>) () -> cachedDatabaseSize );
    }

    @Override
    public void stop()
    {
        registry.remove( databaseSize );
        registry.remove( totalStoreSize );

        if ( updateValuesHandle != null )
        {
            updateValuesHandle.cancel();
            updateValuesHandle = null;
        }
    }

    private void updateCachedValues()
    {
        cachedDatabaseSize = getSize( databaseLayout.databaseDirectory() );
        cachedStoreTotalSize = cachedDatabaseSize + getSize( databaseLayout.getTransactionLogsDirectory() );
    }

    //Paths may overlap
    private long getSize( Path... files )
    {
        Set<Path> visitedFiles = new HashSet<>();
        return Arrays.stream( files ).mapToLong( file -> getSizeInternal( file, visitedFiles ) ).sum();
    }

    private long getSizeInternal( Path file, Set<Path> visitedFiles )
    {
        if ( !visitedFiles.add( file ) )
        {
            return 0L; //dont visit files twice
        }

        if ( Files.isDirectory( file ) )
        {
            Path[] filesInDir = fileSystem.listFiles( file );
            if ( filesInDir == null || filesInDir.length == 0 )
            {
                return 0L;
            }
            return Arrays.stream( filesInDir ).mapToLong( fileInDir -> getSizeInternal( fileInDir, visitedFiles ) ).sum();
        }
        return fileSystem.getFileSize( file );
    }

}
