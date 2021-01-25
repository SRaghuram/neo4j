/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.extension;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.annotation.Annotation;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.UnaryOperator;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.DbmsSupportController;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

public class EnterpriseDbmsSupportController extends DbmsSupportController
{
    public EnterpriseDbmsSupportController( ExtensionContext context )
    {
        super( context );
    }

    @Override
    public void startDbms( String databaseName, UnaryOperator<TestDatabaseManagementServiceBuilder> callback )
    {
        // Find closest configuration
        TestConfiguration enterpriseDbmsExtension = getAnnotatedConfiguration(
                getTestAnnotation( EnterpriseDbmsExtension.class ),
                getTestAnnotation( ImpermanentEnterpriseDbmsExtension.class ) );

        // Create service
        var dbms = buildDbms( enterpriseDbmsExtension, callback );
        var databaseToStart = isNotEmpty( databaseName ) ? databaseName : getDatabaseName( dbms );
        startDatabase( databaseToStart );
    }

    @Override
    public TestDatabaseManagementServiceBuilder createBuilder( Path homeDirectory, FileSystemAbstraction fileSystem )
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( homeDirectory ).setFileSystem( fileSystem );
    }

    @Override
    public void shutdown()
    {
        super.shutdown();
    }

    @Override
    public DbmsController asDbmsController()
    {
        return new DbmsController()
        {
            @Override
            public void restartDbms( String databaseName, UnaryOperator<TestDatabaseManagementServiceBuilder> callback )
            {
                shutdown();
                try
                {
                    startDbms( databaseName, callback );
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
            }

            @Override
            public void restartDatabase( String databaseName )
            {
                EnterpriseDbmsSupportController.this.restartDatabase( databaseName );
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
}
