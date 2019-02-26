/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.internal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.forced_kernel_id;

@PageCacheExtension
class KernelDataTest
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;
    private final Collection<Kernel> kernels = new HashSet<>();

    @AfterEach
    void tearDown()
    {
        Iterator<Kernel> kernelIterator = kernels.iterator();
        while ( kernelIterator.hasNext() )
        {
            Kernel kernel = kernelIterator.next();
            kernelIterator.remove();
            kernel.shutdown();
        }
    }

    @Test
    void shouldGenerateUniqueInstanceIdentifiers()
    {
        // given
        Kernel kernel1 = new Kernel( null );

        // when
        Kernel kernel2 = new Kernel( null );

        // then
        assertNotNull( kernel1.instanceId() );
        assertNotNull( kernel2.instanceId() );
        assertNotEquals( kernel1.instanceId(), kernel2.instanceId() );
    }

    @Test
    void shouldReuseInstanceIdentifiers()
    {
        // given
        Kernel kernel = new Kernel( null );
        String instanceId = kernel.instanceId();
        kernel.shutdown();

        // when
        kernel = new Kernel( null );

        // then
        assertEquals( instanceId, kernel.instanceId() );
    }

    @Test
    void shouldAllowConfigurationOfInstanceId()
    {
        // when
        Kernel kernel = new Kernel( "myInstance" );

        // then
        assertEquals( "myInstance", kernel.instanceId() );
    }

    @Test
    void shouldGenerateInstanceIdentifierWhenNullConfigured()
    {
        // when
        Kernel kernel = new Kernel( null );

        // then
        assertEquals( kernel.instanceId(), kernel.instanceId().trim() );
        assertTrue( kernel.instanceId().length() > 0 );
    }

    @Test
    void shouldGenerateInstanceIdentifierWhenEmptyStringConfigured()
    {
        // when
        Kernel kernel = new Kernel( "" );

        // then
        assertEquals( kernel.instanceId(), kernel.instanceId().trim() );
        assertTrue( kernel.instanceId().length() > 0 );
    }

    @Test
    void shouldNotAllowMultipleInstancesWithTheSameConfiguredInstanceId()
    {
        // given
        new Kernel( "myInstance" );

        // when
        IllegalStateException exception = assertThrows( IllegalStateException.class, () -> new Kernel( "myInstance" ) );
        assertEquals( "There is already a kernel started with unsupported.dbms.kernel_id='myInstance'.", exception.getMessage() );
    }

    @Test
    void shouldAllowReuseOfConfiguredInstanceIdAfterShutdown()
    {
        // given
        new Kernel( "myInstance" ).shutdown();

        // when
        Kernel kernel = new Kernel( "myInstance" );

        // then
        assertEquals( "myInstance", kernel.instanceId() );
    }

    private class Kernel extends KernelData
    {
        Kernel( String desiredId )
        {
            super( fileSystem, pageCache, new File( DEFAULT_DATABASE_NAME ), Config.defaults( forced_kernel_id, desiredId ) );
            kernels.add( this );
        }

        @Override
        public void shutdown()
        {
            super.shutdown();
            kernels.remove( this );
        }
    }
}
