/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.extension;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.UnaryOperator;

import org.neo4j.collection.Dependencies;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.DbmsSupportController;

public class EnterpriseDbmsSupportController extends DbmsSupportController
{
    public EnterpriseDbmsSupportController( ExtensionContext context )
    {
        super( context );
    }

    @Override
    public void startDbms( UnaryOperator<TestDatabaseManagementServiceBuilder> callback ) throws Exception
    {
        // Create and manage TestDirectoryExtension our self
        // The caveat is that the order of postProcessTestInstance and beforeAll changes if you change TestInstance.Lifecycle
        injectManagedTestDirectory();

        // Find closest configuration
        TestConfiguration enterpriseDbmsExtension = getAnnotatedConfiguration(
                getTestAnnotation( EnterpriseDbmsExtension.class ),
                getTestAnnotation( ImpermanentEnterpriseDbmsExtension.class ) );

        // Create service
        DatabaseManagementService dbms = buildDbms( enterpriseDbmsExtension.configurationCallback, callback );

        var dbmsDependency = new Dependencies();
        dbmsDependency.satisfyDependencies( dbms );
        injectDependencies( dbmsDependency );
    }

    @Override
    public TestDatabaseManagementServiceBuilder createBuilder( File homeDirectory, FileSystemAbstraction fileSystem )
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( homeDirectory ).setFileSystem( fileSystem );
    }

    @Override
    public void shutdown()
    {
        super.shutdown();
        removeManagedTestDirectory();
    }

    @Override
    public DbmsController asDbmsController()
    {
        return new DbmsController()
        {
            @Override
            public void restartDbms( UnaryOperator<TestDatabaseManagementServiceBuilder> callback )
            {
                shutdown();
                try
                {
                    startDbms( callback );
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
            }

            @Override
            public void restartDatabase()
            {
                EnterpriseDbmsSupportController.this.restartDatabase();
            }
        };
    }

    @SafeVarargs
    private static TestConfiguration getAnnotatedConfiguration( Optional<? extends Annotation>... options )
    {
        Annotation[] annotations = Arrays.stream( options ).flatMap( Optional::stream ).toArray( Annotation[]::new );

        if ( annotations.length > 1 )
        {
            throw new IllegalArgumentException( "Multiple DBMS annotations found for the configuration: " + Arrays.toString( annotations ) + "." );
        }

        if ( annotations.length == 1 )
        {
            if ( annotations[0] instanceof EnterpriseDbmsExtension )
            {
                EnterpriseDbmsExtension annotation = (EnterpriseDbmsExtension) annotations[0];
                return new TestConfiguration( annotation.configurationCallback() );
            }
            if ( annotations[0] instanceof ImpermanentEnterpriseDbmsExtension )
            {
                ImpermanentEnterpriseDbmsExtension annotation = (ImpermanentEnterpriseDbmsExtension) annotations[0];
                return new TestConfiguration( annotation.configurationCallback() );
            }
        }

        throw new IllegalArgumentException( String.format( "No annotation of type \"%s\" or \"%s\" found.",
                EnterpriseDbmsExtension.class.getSimpleName(), ImpermanentEnterpriseDbmsExtension.class.getSimpleName() )  );
    }

    private static class TestConfiguration
    {
        private final String configurationCallback;

        private TestConfiguration( String configurationCallback )
        {
            this.configurationCallback = configurationCallback;
        }
    }
}
