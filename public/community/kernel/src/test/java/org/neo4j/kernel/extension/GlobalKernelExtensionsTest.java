/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.extension;

import org.junit.Test;

import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.scheduler.JobScheduler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.helpers.collection.Iterables.iterable;

public class GlobalKernelExtensionsTest
{
    @Test
    public void shouldConsultUnsatisfiedDependencyHandlerOnMissingDependencies()
    {
        // GIVEN
        KernelContext context = mock( KernelContext.class );
        KernelExtensionFailureStrategy handler = mock( KernelExtensionFailureStrategy.class );
        Dependencies dependencies = new Dependencies(); // that hasn't got anything.
        TestingExtensionFactory extensionFactory = new TestingExtensionFactory();
        GlobalKernelExtensions extensions = new GlobalKernelExtensions( context, iterable( extensionFactory ), dependencies, handler );

        // WHEN
        LifeSupport life = new LifeSupport();
        life.add( extensions );
        try
        {
            life.start();

            // THEN
            verify( handler ).handle( eq( extensionFactory ), any( UnsatisfiedDependencyException.class ) );
        }
        finally
        {
            life.shutdown();
        }
    }

    @Test
    public void shouldConsultUnsatisfiedDependencyHandlerOnFailingDependencyClasses()
    {
        // GIVEN
        KernelContext context = mock( KernelContext.class );
        KernelExtensionFailureStrategy handler = mock( KernelExtensionFailureStrategy.class );
        Dependencies dependencies = new Dependencies(); // that hasn't got anything.
        UninitializableKernelExtensionFactory extensionFactory = new UninitializableKernelExtensionFactory();
        GlobalKernelExtensions extensions = new GlobalKernelExtensions( context, iterable( extensionFactory ), dependencies, handler );

        // WHEN
        LifeSupport life = new LifeSupport();
        life.add( extensions );
        try
        {
            life.start();

            // THEN
            verify( handler ).handle( eq( extensionFactory ), any( IllegalArgumentException.class ) );
        }
        finally
        {
            life.shutdown();
        }
    }

    private interface TestingDependencies
    {
        // Just some dependency
        JobScheduler jobScheduler();
    }

    private static class TestingExtensionFactory extends KernelExtensionFactory<TestingDependencies>
    {
        TestingExtensionFactory()
        {
            super( "testing" );
        }

        @Override
        public Lifecycle newInstance( KernelContext context, TestingDependencies dependencies )
        {
            return new TestingExtension( dependencies.jobScheduler() );
        }
    }

    private static class TestingExtension extends LifecycleAdapter
    {
        TestingExtension( JobScheduler jobScheduler )
        {
            // We don't need it right now
        }
    }
}
