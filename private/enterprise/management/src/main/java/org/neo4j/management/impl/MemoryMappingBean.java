/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management.impl;

import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.management.MemoryMapping;
import org.neo4j.management.WindowPoolInfo;

@Deprecated
@Service.Implementation( ManagementBeanProvider.class )
public final class MemoryMappingBean extends ManagementBeanProvider
{
    public MemoryMappingBean()
    {
        super( MemoryMapping.class );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new MemoryMappingImpl( management );
    }

    @Override
    protected Neo4jMBean createMXBean( ManagementData management )
    {
        return new MemoryMappingImpl( management, true );
    }

    private static class MemoryMappingImpl extends Neo4jMBean implements MemoryMapping
    {
        private final NeoStoreDataSource datasource;

        MemoryMappingImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );
            this.datasource = neoDataSource( management );
        }

        private NeoStoreDataSource neoDataSource( ManagementData management )
        {
            return management.getKernelData().getDataSourceManager().getDataSource();
        }

        MemoryMappingImpl( ManagementData management, boolean isMxBean )
        {
            super( management, isMxBean );
            this.datasource = neoDataSource( management );
        }

        @Deprecated
        @Override
        public WindowPoolInfo[] getMemoryPools()
        {
            return getMemoryPoolsImpl( datasource );
        }

        @Deprecated
        public static WindowPoolInfo[] getMemoryPoolsImpl( NeoStoreDataSource datasource )
        {
            return new WindowPoolInfo[0];
        }
    }
}
