/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.extension;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.rule.TestDirectory;


public class EnterpriseDbmsSupportExtension extends DbmsSupportExtension implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback
{
    private static final String TEST_DIRECTORY_EXTENSION_KEY = "testDirectoryExtension";

    @Override
    public void beforeAll( ExtensionContext context ) throws Exception
    {
        Object testInstance = context.getRequiredTestInstance();

        // Create and manage TestDirectoryExtension our self
        // The caveat is that the order of postProcessTestInstance and beforeAll changes if you change TestInstance.Lifecycle
        TestDirectorySupportExtension testDirectorySupportExtension = new TestDirectorySupportExtension();
        testDirectorySupportExtension.postProcessTestInstance( testInstance, context );
        testDirectorySupportExtension.prepare( context );
        getStore( context ).put( TEST_DIRECTORY_EXTENSION_KEY, testDirectorySupportExtension );

        TestDirectory testDirectory = getTestDirectory( context );
        FileSystemAbstraction fileSystem = testDirectory.getFileSystem();

        // Find closest configuration
        TestConfiguration enterpriseDbmsExtension = getAnnotatedConfiguration( context );

        // Create service
        TestDatabaseManagementServiceBuilder builder =
                new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homeDir() ).setFileSystem( fileSystem );
        maybeInvokeCallback( testInstance, builder, enterpriseDbmsExtension.configurationCallback );
        DatabaseManagementService dbms = builder.build();

        // Save in context
        ExtensionContext.Store store = getStore( context );
        store.put( DBMS, dbms );

        // Inject dbms
        injectInstance( testInstance, dbms, DatabaseManagementService.class );
    }

    @Override
    public void afterAll( ExtensionContext context )
    {
        DatabaseManagementService dbms = getStore( context ).remove( DBMS, DatabaseManagementService.class );
        dbms.shutdown();

        TestDirectorySupportExtension testDirectorySupportExtension =
                getStore( context ).remove( TEST_DIRECTORY_EXTENSION_KEY, TestDirectorySupportExtension.class );
        testDirectorySupportExtension.afterEach( context );
    }

    @Override
    public void beforeEach( ExtensionContext context )
    {
        // Create a new database for each test method
        DatabaseManagementService dbms = getDbmsFromStore( context );
        String uniqueTestName = getUniqueTestName( context );
        dbms.createDatabase( uniqueTestName );
        GraphDatabaseAPI db = (GraphDatabaseAPI) dbms.database( uniqueTestName );

        // Inject db
        Object testInstance = context.getRequiredTestInstance();
        injectInstance( testInstance, db, GraphDatabaseService.class );
        injectInstance( testInstance, db, GraphDatabaseAPI.class );
    }

    @Override
    public void afterEach( ExtensionContext context )
    {
        DatabaseManagementService dbms = getDbmsFromStore( context );
        dbms.shutdownDatabase( getUniqueTestName( context ) );
    }

    private static String getUniqueTestName( ExtensionContext context )
    {
        // Test name is restricted to 31 characters since database name must be less than or equal to 63 characters
        String testName = context.getRequiredTestMethod().getName();
        testName = testName.substring( 0, Math.min( 31, testName.length() ) );
        return testName + DigestUtils.md5Hex( context.getUniqueId() );
    }

    private static DatabaseManagementService getDbmsFromStore( ExtensionContext context )
    {
        return getStore( context ).get( DBMS, DatabaseManagementService.class );
    }

    private static TestConfiguration getAnnotatedConfiguration( ExtensionContext context )
    {
        EnterpriseDbmsExtension enterpriseDbmsExtension =
                context.getRequiredTestClass().getAnnotation( EnterpriseDbmsExtension.class );
        ImpermanentEnterpriseDbmsExtension impermanentEnterpriseDbmsExtension =
                context.getRequiredTestClass().getAnnotation( ImpermanentEnterpriseDbmsExtension.class );
        if ( enterpriseDbmsExtension == null && impermanentEnterpriseDbmsExtension == null )
        {
            throw new IllegalArgumentException( String.format( "No annotation of type \"%s\" or \"%s\" found.", EnterpriseDbmsExtension.class.getSimpleName(),
                    ImpermanentEnterpriseDbmsExtension.class.getSimpleName() )  );
        }

        if ( enterpriseDbmsExtension != null && impermanentEnterpriseDbmsExtension != null )
        {
            throw new IllegalArgumentException( String.format( "Can't mix \"%s\" and \"%s\" annotations.", EnterpriseDbmsExtension.class.getSimpleName(),
                    ImpermanentEnterpriseDbmsExtension.class.getSimpleName() ) );
        }

        if ( enterpriseDbmsExtension != null )
        {
            return new TestConfiguration( enterpriseDbmsExtension.configurationCallback() );
        }

        return new TestConfiguration( impermanentEnterpriseDbmsExtension.configurationCallback() );
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
