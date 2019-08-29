/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Database store size metrics" )
public class StoreSizeMetrics extends LifecycleAdapter
{
    private static final String PREFIX = "store.size";

    @Documented( "The total size of the database store" )
    public static final String TOTAL_STORE_SIZE = name( PREFIX, "total" );

    private final MetricRegistry registry;
    private final String totalStoreSize;
    private final JobScheduler scheduler;
    private final FileSystemAbstraction fileSystem;
    private final DatabaseLayout databaseLayout;

    private volatile JobHandle updateValuesHandle;
    private volatile long cachedStoreTotalSize = -1L;

    public StoreSizeMetrics( String metricsPrefix, MetricRegistry registry, JobScheduler scheduler, FileSystemAbstraction fileSystemAbstraction,
            DatabaseLayout databaseLayout )
    {
        this.registry = registry;
        this.totalStoreSize = name( metricsPrefix, TOTAL_STORE_SIZE );
        this.scheduler = scheduler;
        this.fileSystem = fileSystemAbstraction;
        this.databaseLayout = databaseLayout;
    }

    @Override
    public void start()
    {
        if ( updateValuesHandle == null )
        {
            updateValuesHandle = scheduler.scheduleRecurring( Group.FILE_IO_HELPER, this::updateCachedValues, 10, TimeUnit.MINUTES );
        }
        registry.register( totalStoreSize, (Gauge<Long>) () -> cachedStoreTotalSize );
    }

    @Override
    public void stop()
    {
        registry.remove( totalStoreSize );

        if ( updateValuesHandle != null )
        {
            updateValuesHandle.cancel( true );
            updateValuesHandle = null;
        }
    }

    private void updateCachedValues()
    {
      cachedStoreTotalSize = getSize( databaseLayout.databaseDirectory() );
    }

    private long getSize( File file )
    {
        if ( file.isDirectory() )
        {
            File[] files = fileSystem.listFiles( file );
            if ( files == null )
            {
                return 0L;
            }
            return Arrays.stream( files ).mapToLong( this::getSize ).sum();
        }
        return fileSystem.getFileSize( file );
    }

}
