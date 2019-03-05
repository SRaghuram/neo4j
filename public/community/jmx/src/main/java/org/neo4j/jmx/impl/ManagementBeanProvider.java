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
package org.neo4j.jmx.impl;

import java.util.Collection;
import java.util.Collections;
import javax.management.DynamicMBean;
import javax.management.NotCompliantMBeanException;

import org.neo4j.annotations.service.Service;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.service.NamedService;

@Service
@Deprecated
public abstract class ManagementBeanProvider implements NamedService
{
    final Class<?> beanInterface;

    public ManagementBeanProvider( Class<?> beanInterface )
    {
        if ( DynamicMBean.class.isAssignableFrom( beanInterface ) )
        {
            beanInterface = DynamicMBean.class;
        }
        this.beanInterface = beanInterface;
    }

    @Override
    public String getName()
    {
        return ManagementSupport.beanName( beanInterface );
    }

    private Iterable<? extends Neo4jMBean> createMBeans( ManagementData management ) throws NotCompliantMBeanException
    {
        return singletonOrNone( createMBean( management ) );
    }

    private Iterable<? extends Neo4jMBean> createMXBeans( ManagementData management ) throws NotCompliantMBeanException
    {
        Class<?> implClass;
        try
        {
            implClass = getClass().getDeclaredMethod( "createMBeans", ManagementData.class ).getDeclaringClass();
        }
        catch ( Exception e )
        {
            implClass = ManagementBeanProvider.class; // Assume no override
        }
        if ( implClass != ManagementBeanProvider.class )
        { // if createMBeans is overridden, delegate to it
            return createMBeans( management );
        }
        else
        { // otherwise delegate to the createMXBean method and create a list
            return singletonOrNone( createMXBean( management ) );
        }
    }

    protected abstract Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException;

    protected Neo4jMBean createMXBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return createMBean( management );
    }

    final Iterable<? extends Neo4jMBean> loadBeans( KernelData kernel, Database database, ManagementSupport support )
            throws Exception
    {
        if ( support.supportsMxBeans() )
        {
            return createMXBeans( new ManagementData( this, kernel, database, support ) );
        }
        else
        {
            return createMBeans( new ManagementData( this, kernel, database, support ) );
        }
    }

    private static Collection<? extends Neo4jMBean> singletonOrNone( Neo4jMBean mbean )
    {
        if ( mbean == null )
        {
            return Collections.emptySet();
        }
        return Collections.singleton( mbean );
    }
}
