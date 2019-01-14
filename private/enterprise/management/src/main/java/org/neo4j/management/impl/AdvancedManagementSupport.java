/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management.impl;

import javax.management.DynamicMBean;
import javax.management.ObjectName;

import org.neo4j.jmx.impl.ConfigurationBean;
import org.neo4j.jmx.impl.KernelBean;
import org.neo4j.jmx.impl.ManagementSupport;

@Deprecated
abstract class AdvancedManagementSupport extends ManagementSupport
{
    @Override
    protected final boolean supportsMxBeans()
    {
        return true;
    }

    @Override
    protected final <T> T makeProxy( KernelBean kernel, ObjectName name, Class<T> beanInterface )
    {
        return BeanProxy.load( getMBeanServer(), beanInterface, name );
    }

    @Override
    protected String getBeanName( Class<?> beanInterface )
    {
        if ( beanInterface == DynamicMBean.class )
        {
            return ConfigurationBean.CONFIGURATION_MBEAN_NAME;
        }
        return KernelProxy.beanName( beanInterface );
    }

    @Override
    protected ObjectName createObjectName( String instanceId, String beanName, boolean query, String... extraNaming )
    {
        return query ? KernelProxy.createObjectNameQuery( instanceId, beanName, extraNaming ) : KernelProxy
                .createObjectName( instanceId, beanName, extraNaming );
    }
}
