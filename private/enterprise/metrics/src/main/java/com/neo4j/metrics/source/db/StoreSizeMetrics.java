/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Documented( ".Database store size metrics" )
public class StoreSizeMetrics extends LifecycleAdapter
{
    public static final Duration UPDATE_INTERVAL = Duration.ofMinutes( 10 );

    private static final String PREFIX = "store.size";

    @Documented( "The total size of the database and transaction logs. (gauge)" )
    private static final String TOTAL_STORE_SIZE = name( PREFIX, "total" );
    @Documented( "The size of the database. (gauge)" )
    private static final String DATABASE_SIZE = name( PREFIX, "database" );

    private final MetricRegistry registry;
    private final String totalStoreSize;
    private final String databaseSize;
    private final JobScheduler scheduler;
    private final FileSystemAbstraction fileSystem;
    private final DatabaseLayout databaseLayout;

    private volatile JobHandle updateValuesHandle;
    private volatile long cachedStoreTotalSize = -1L;
    private volatile long cachedDatabaseSize = -1L;

    public StoreSizeMetrics( String metricsPrefix, MetricRegistry registry, JobScheduler scheduler, FileSystemAbstraction fileSystemAbstraction,
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
            updateValuesHandle = scheduler.scheduleRecurring( Group.FILE_IO_HELPER, this::updateCachedValues, UPDATE_INTERVAL.toMillis(), MILLISECONDS );
        }
        registry.register( totalStoreSize, (Gauge<Long>) () -> cachedStoreTotalSize );
        registry.register( databaseSize, (Gauge<Long>) () -> cachedDatabaseSize );
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
        cachedDatabaseSize = getSize( databaseLayout.databaseDirectory().toFile() );
        cachedStoreTotalSize = cachedDatabaseSize + getSize( databaseLayout.getTransactionLogsDirectory().toFile() );
    }

    //Paths may overlap
    private long getSize( File... files )
    {
        Set<File> visitedFiles = new HashSet<>();
        return Arrays.stream( files ).mapToLong( file -> getSizeInternal( file, visitedFiles ) ).sum();
    }

    private long getSizeInternal( File file, Set<File> visitedFiles )
    {
        if ( !visitedFiles.add( file ) )
        {
            return 0L; //dont visit files twice
        }

        if ( file.isDirectory() )
        {
            File[] filesInDir = fileSystem.listFiles( file );
            if ( filesInDir == null || filesInDir.length == 0 )
            {
                return 0L;
            }
            return Arrays.stream( filesInDir ).mapToLong( fileInDir -> getSizeInternal( fileInDir, visitedFiles ) ).sum();
        }
        return fileSystem.getFileSize( file );
    }

}
