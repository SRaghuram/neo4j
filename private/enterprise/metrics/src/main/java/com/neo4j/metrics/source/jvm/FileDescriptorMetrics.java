/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.jvm;

import com.codahale.metrics.Gauge;
import com.neo4j.metrics.metric.MetricsRegister;
import org.apache.commons.lang3.SystemUtils;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.io.os.OsBeanUtil;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".JVM file descriptor metrics." )
public class FileDescriptorMetrics extends JvmMetrics
{
    @Documented( "The current number of open file descriptors. (gauge)" )
    private static final String FILE_COUNT_TEMPLATE = name( VM_NAME_PREFIX, "file.descriptors.count" );
    @Documented( "The maximum number of open file descriptors. (gauge)" )
    private static final String FILE_MAXIMUM_TEMPLATE = name( VM_NAME_PREFIX, "file.descriptors.maximum" );

    private final String fileCount;
    private final String fileMaximum;

    private final MetricsRegister registry;

    public FileDescriptorMetrics( String metricsPrefix, MetricsRegister registry )
    {
        this.registry = registry;
        this.fileCount = name( metricsPrefix, FILE_COUNT_TEMPLATE );
        this.fileMaximum = name( metricsPrefix, FILE_MAXIMUM_TEMPLATE );
    }

    @Override
    public void start()
    {
        if ( SystemUtils.IS_OS_UNIX )
        {
            registry.register( fileCount, () -> (Gauge<Long>) OsBeanUtil::getOpenFileDescriptors );
            registry.register( fileMaximum, () -> (Gauge<Long>) OsBeanUtil::getMaxFileDescriptors );
        }
        else
        {
            registry.register( fileCount, () -> (Gauge<Long>) () -> -1L );
            registry.register( fileMaximum, () -> (Gauge<Long>) () -> -1L );
        }
    }

    @Override
    public void stop()
    {
        registry.remove( fileCount );
        registry.remove( fileMaximum );
    }
}
