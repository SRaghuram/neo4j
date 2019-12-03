/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.neo4j.test.extension.StatefullFieldExtension;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

class DriverFactoryExtension extends StatefullFieldExtension<DriverFactory> implements AfterEachCallback
{
    private static final String DRIVER = "driver";
    private static final ExtensionContext.Namespace DRIVER_NAMESPACE = ExtensionContext.Namespace.create( DRIVER );

    private TestDirectory getTestDirectory( ExtensionContext context )
    {
        TestDirectory testDir = context
                .getStore( TestDirectorySupportExtension.TEST_DIRECTORY_NAMESPACE )
                .get( TestDirectorySupportExtension.TEST_DIRECTORY, TestDirectory.class );
        if ( testDir == null )
        {
            throw new IllegalStateException(
                    TestDirectorySupportExtension.class.getSimpleName() + " not in scope, make sure to add it before the " + getClass().getSimpleName() );
        }
        return testDir;
    }

    @Override
    protected String getFieldKey()
    {
        return DRIVER;
    }

    @Override
    protected Class<DriverFactory> getFieldType()
    {
        return DriverFactory.class;
    }

    @Override
    protected DriverFactory createField( ExtensionContext extensionContext )
    {
        return new DriverFactory( getTestDirectory( extensionContext ) );
    }

    @Override
    protected ExtensionContext.Namespace getNameSpace()
    {
        return DRIVER_NAMESPACE;
    }

    @Override
    public void afterEach( ExtensionContext context )
    {
        if ( getLifecycle( context ) == PER_METHOD )
        {
            getStoredValue( context ).close();
        }
    }

    private TestInstance.Lifecycle getLifecycle( ExtensionContext context )
    {
        return context.getTestInstanceLifecycle().orElse( PER_METHOD );
    }

    @Override
    public void afterAll( ExtensionContext context ) throws Exception
    {
        try
        {
            getStoredValue( context );
        }
        finally
        {
            super.afterAll( context );
        }
    }
}
