/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.driver;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;

import org.neo4j.test.extension.StatefulFieldExtension;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;

class ClusterCheckerFactoryExtension extends StatefulFieldExtension<ClusterCheckerFactory> implements AfterEachCallback
{
    private static final String CLUSTER_CHECKER = "cluster_checker";
    private static final ExtensionContext.Namespace CLUSTER_CHECKER_NAMESPACE = ExtensionContext.Namespace.create( CLUSTER_CHECKER );

    private DriverFactory getDriverFactory( ExtensionContext context )
    {
        DriverFactory driverFactory = context
                .getStore( DriverFactoryExtension.DRIVER_NAMESPACE )
                .get( DriverFactoryExtension.DRIVER, DriverFactory.class );
        if ( driverFactory == null )
        {
            throw new IllegalStateException(
                    TestDirectorySupportExtension.class.getSimpleName() + " not in scope, make sure to add it before the " + getClass().getSimpleName() );
        }
        return driverFactory;
    }

    @Override
    public void afterEach( ExtensionContext context ) throws IOException
    {
        if ( context.getTestInstanceLifecycle().filter( lifecycle -> lifecycle == TestInstance.Lifecycle.PER_METHOD ).isPresent() )
        {
            getStoredValue( context ).close();
        }
    }

    @Override
    public void afterAll( ExtensionContext context ) throws Exception
    {
        getStoredValue( context ).close();
        super.afterAll( context );
    }

    @Override
    protected String getFieldKey()
    {
        return CLUSTER_CHECKER;
    }

    @Override
    protected Class<ClusterCheckerFactory> getFieldType()
    {
        return ClusterCheckerFactory.class;
    }

    @Override
    protected ClusterCheckerFactory createField( ExtensionContext extensionContext )
    {
        return new ClusterCheckerFactory( getDriverFactory( extensionContext ) );
    }

    @Override
    protected ExtensionContext.Namespace getNameSpace()
    {
        return CLUSTER_CHECKER_NAMESPACE;
    }
}
