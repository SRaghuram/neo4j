/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.internal.helpers.Strings.escape;

@ExtendWith( {TestDirectoryExtension.class} )
class SecurityDDLLoggingIT
{
    private DatabaseManagementService managementService;
    private GraphDatabaseFacade database;
    private File logFilename;
    private StubLoginContext adminContext = new StubLoginContext( "fakeAdmin", AccessMode.Static.FULL );

    @Inject
    protected TestDirectory testDirectory;

    @BeforeEach
    void setUp()
    {
        File logsDirectory = new File( testDirectory.storeDir(), "logs" );
        logFilename = new File( logsDirectory, "security.log" );
        AssertableLogProvider inMemoryLog = new AssertableLogProvider();
        managementService = new TestCommercialDatabaseManagementServiceBuilder( testDirectory.storeDir() )
                .setInternalLogProvider( inMemoryLog )
                .setFileSystem( testDirectory.getFileSystem() )
                .impermanent()
                .setConfig( auth_enabled, true )
                .build();
        database = (GraphDatabaseFacade) managementService.database( SYSTEM_DATABASE_NAME );
    }

    @AfterEach
    void tearDown()
    {
        managementService.shutdown();
    }

    @Test
    void shouldLogCreateUser() throws IOException
    {
        // WHEN
        execute( adminContext, "CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED" );
        execute( adminContext, "CREATE USER baz SET PASSWORD $password", Map.of( "password", "secret" ) );

        // THEN
        List<String> logLines = readAllLines( logFilename );
        assertThat( logLines, hasSize( 2 ) );
        assertThat( logLines.get( 0 ), containsString( withSubject( adminContext, "CREATE USER foo SET PASSWORD '******' CHANGE NOT REQUIRED" ) ) );
        assertThat( logLines.get( 1 ), containsString( withSubject( adminContext, "CREATE USER baz SET PASSWORD $password CHANGE REQUIRED" ) ) );
    }

    @Test
    void shouldLogDropUser() throws IOException
    {
        // GIVEN
        execute( adminContext, "CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED" );

        // WHEN
        execute( adminContext, "DROP USER foo" );

        // THEN
        List<String> logLines = readAllLines( logFilename );
        assertThat( logLines, hasSize( 2 ) ); // First line is from setting up the user to delete later
        assertThat( logLines.get( 1 ), containsString( withSubject( adminContext, "DROP USER foo" ) ) );
    }

    @Test
    void shouldLogAlterUser() throws IOException
    {
        // GIVEN
        execute( adminContext, "CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED" );

        // WHEN
        execute( adminContext, "ALTER USER foo SET PASSWORD 'baz' CHANGE REQUIRED" );
        execute( adminContext, "ALTER USER foo SET STATUS SUSPENDED" );

        // THEN
        List<String> logLines = readAllLines( logFilename );
        assertThat( logLines, hasSize( 3 ) ); // First line is from setting up the user to alter later
        assertThat( logLines.get( 1 ), containsString( withSubject( adminContext, "ALTER USER foo SET PASSWORD '******' CHANGE REQUIRED" ) ) );
        assertThat( logLines.get( 2 ), containsString( withSubject( adminContext, "ALTER USER foo SET STATUS SUSPENDED" ) ) );
    }

    @Test
    void shouldLogSetOwnPassword() throws IOException
    {
        // GIVEN
        // adminContext is a user that doesn't exist in the system graph and does not have a password.
        // This test will still show the logging of the command but nothing will actually get executed.

        // WHEN
        execute( adminContext, "ALTER CURRENT USER SET PASSWORD FROM '???' TO 'baz'" );

        // THEN
        List<String> logLines = readAllLines( logFilename );
        assertThat( logLines, hasSize( 1 ) );
        assertThat( logLines.get( 0 ), containsString( withSubject( adminContext, "ALTER CURRENT USER SET PASSWORD FROM '******' TO '******'" ) ) );
    }

    @Test
    void shouldLogCreateRole() throws IOException
    {
        // WHEN
        execute( adminContext, "CREATE ROLE foo" );

        // THEN
        List<String> logLines = readAllLines( logFilename );
        assertThat( logLines, hasSize( 1 ) );
        assertThat( logLines.get( 0 ), containsString( withSubject( adminContext, "CREATE ROLE foo" ) ) );
    }

    @Test
    void shouldLogCreateRoleAsCopy() throws IOException
    {
        // WHEN
        execute( adminContext, "CREATE ROLE foo AS COPY OF admin" );

        // THEN
        List<String> logLines = readAllLines( logFilename );
        assertThat( logLines, hasSize( 1 ) );
        assertThat( logLines.get( 0 ), containsString( withSubject( adminContext, "CREATE ROLE foo AS COPY OF admin" ) ) );
    }

    @Test
    void shouldLogDropRole() throws IOException
    {
        // GIVEN
        execute( adminContext, "CREATE ROLE foo" );

        // WHEN
        execute( adminContext, "DROP ROLE foo" );

        // THEN
        List<String> logLines = readAllLines( logFilename );
        assertThat( logLines, hasSize( 2 ) );
        assertThat( logLines.get( 1 ), containsString( withSubject( adminContext, "DROP ROLE foo" ) ) );
    }

    @Test
    void shouldLogGrantRole() throws IOException
    {
        // GIVEN
        execute( adminContext, "CREATE ROLE foo" );
        execute( adminContext, "CREATE ROLE bar" );
        execute( adminContext, "CREATE USER alice SET PASSWORD 'abc'" );
        execute( adminContext, "CREATE USER bob SET PASSWORD 'abc'" );

        // WHEN
        execute( adminContext, "GRANT ROLE foo TO alice" );
        execute( adminContext, "GRANT ROLE foo,bar TO alice,bob" );

        // THEN
        List<String> logLines = readAllLines( logFilename );
        assertThat( logLines, hasSize( 6 ) );
        assertThat( logLines.get( 4 ), containsString( withSubject( adminContext, "GRANT ROLE foo TO alice" ) ) );
        assertThat( logLines.get( 5 ), containsString( withSubject( adminContext, "GRANT ROLES foo, bar TO alice, bob" ) ) );
    }

    @Test
    void shouldLogRevokeRole() throws IOException
    {
        // GIVEN
        execute( adminContext, "CREATE ROLE foo" );
        execute( adminContext, "CREATE ROLE bar" );
        execute( adminContext, "CREATE USER alice SET PASSWORD 'abc'" );
        execute( adminContext, "CREATE USER bob SET PASSWORD 'abc'" );
        execute( adminContext, "GRANT ROLE foo TO alice" );
        execute( adminContext, "GRANT ROLE foo,bar TO bob" );

        // WHEN
        execute( adminContext, "REVOKE ROLE foo FROM alice" );
        execute( adminContext, "REVOKE ROLE foo,bar FROM bob" );

        // THEN
        List<String> logLines = readAllLines( logFilename );
        assertThat( logLines, hasSize( 8 ) );
        assertThat( logLines.get( 6 ), containsString( withSubject( adminContext, "REVOKE ROLE foo FROM alice" ) ) );
        assertThat( logLines.get( 7 ), containsString( withSubject( adminContext, "REVOKE ROLES foo, bar FROM bob" ) ) );
    }

    @Test
    void shouldLogGrantTraverse() throws IOException
    {
        // GIVEN
        execute( adminContext, "CREATE ROLE foo" );

        // WHEN
        execute( adminContext, "GRANT TRAVERSE ON GRAPH * TO foo" );
        execute( adminContext, "GRANT TRAVERSE ON GRAPH * NODES A,B TO foo" );
        execute( adminContext, "GRANT TRAVERSE ON GRAPH * RELATIONSHIPS C,D TO foo" );

        // THEN
        List<String> logLines = readAllLines( logFilename );
        assertThat( logLines, hasSize( 4 ) );
        assertThat( logLines.get( 1 ), containsString( withSubject( adminContext, "GRANT TRAVERSE ON GRAPH * ELEMENTS * (*) TO foo" ) ) );
        assertThat( logLines.get( 2 ), containsString( withSubject( adminContext, "GRANT TRAVERSE ON GRAPH * NODES A, B (*) TO foo" ) ) );
        assertThat( logLines.get( 3 ), containsString( withSubject( adminContext, "GRANT TRAVERSE ON GRAPH * RELATIONSHIPS C, D (*) TO foo" ) ) );
    }

    @Test
    void shouldLogGrantReadMatch() throws IOException
    {
        // GIVEN
        execute( adminContext, "CREATE ROLE foo" );

        // WHEN
        execute( adminContext, "GRANT MATCH (*) ON GRAPH * TO foo" );
        execute( adminContext, "GRANT MATCH (bar,baz) ON GRAPH * NODES A,B TO foo" );
        execute( adminContext, "GRANT MATCH (bar,baz) ON GRAPH * RELATIONSHIPS C,D TO foo" );
        execute( adminContext, "GRANT READ (*) ON GRAPH * TO foo" );
        execute( adminContext, "GRANT READ (bar,baz) ON GRAPH * NODES A,B TO foo" );
        execute( adminContext, "GRANT READ (bar,baz) ON GRAPH * RELATIONSHIPS C,D TO foo" );

        // THEN
        List<String> logLines = readAllLines( logFilename );
        assertThat( logLines, hasSize( 7 ) );
        assertThat( logLines.get( 1 ), containsString( withSubject( adminContext, "GRANT MATCH (*) ON GRAPH * ELEMENTS * (*) TO foo" ) ) );
        assertThat( logLines.get( 2 ), containsString( withSubject( adminContext, "GRANT MATCH (bar, baz) ON GRAPH * NODES A, B (*) TO foo" ) ) );
        assertThat( logLines.get( 3 ), containsString( withSubject( adminContext, "GRANT MATCH (bar, baz) ON GRAPH * RELATIONSHIPS C, D (*) TO foo" ) ) );
        assertThat( logLines.get( 4 ), containsString( withSubject( adminContext, "GRANT READ (*) ON GRAPH * ELEMENTS * (*) TO foo" ) ) );
        assertThat( logLines.get( 5 ), containsString( withSubject( adminContext, "GRANT READ (bar, baz) ON GRAPH * NODES A, B (*) TO foo" ) ) );
        assertThat( logLines.get( 6 ), containsString( withSubject( adminContext, "GRANT READ (bar, baz) ON GRAPH * RELATIONSHIPS C, D (*) TO foo" ) ) );
    }

    @Test
    void shouldLogGrantWrite() throws IOException
    {
        // GIVEN
        execute( adminContext, "CREATE ROLE foo" );

        // WHEN
        execute( adminContext, "GRANT WRITE (*) ON GRAPH * TO foo" );

        // THEN
        List<String> logLines = readAllLines( logFilename );
        assertThat( logLines, hasSize( 2 ) );
        assertThat( logLines.get( 1 ), containsString( withSubject( adminContext, "GRANT WRITE (*) ON GRAPH * ELEMENTS * (*) TO foo" ) ) );
    }

    @Test
    void shouldLogDenyTraverse() throws IOException
    {
        // GIVEN
        execute( adminContext, "CREATE ROLE foo" );

        // WHEN
        execute( adminContext, "DENY TRAVERSE ON GRAPH * TO foo" );
        execute( adminContext, "DENY TRAVERSE ON GRAPH * NODES A,B TO foo" );
        execute( adminContext, "DENY TRAVERSE ON GRAPH * RELATIONSHIPS C,D TO foo" );

        // THEN
        List<String> logLines = readAllLines( logFilename );
        assertThat( logLines, hasSize( 4 ) );
        assertThat( logLines.get( 1 ), containsString( withSubject( adminContext, "DENY TRAVERSE ON GRAPH * ELEMENTS * (*) TO foo" ) ) );
        assertThat( logLines.get( 2 ), containsString( withSubject( adminContext, "DENY TRAVERSE ON GRAPH * NODES A, B (*) TO foo" ) ) );
        assertThat( logLines.get( 3 ), containsString( withSubject( adminContext, "DENY TRAVERSE ON GRAPH * RELATIONSHIPS C, D (*) TO foo" ) ) );
    }

    @Test
    void shouldLogDenyReadMatch() throws IOException
    {
        // GIVEN
        execute( adminContext, "CREATE ROLE foo" );

        // WHEN
        execute( adminContext, "DENY MATCH (*) ON GRAPH * TO foo" );
        execute( adminContext, "DENY MATCH (bar,baz) ON GRAPH * NODES A,B TO foo" );
        execute( adminContext, "DENY MATCH (bar,baz) ON GRAPH * RELATIONSHIPS C,D TO foo" );
        execute( adminContext, "DENY READ (*) ON GRAPH * TO foo" );
        execute( adminContext, "DENY READ (bar,baz) ON GRAPH * NODES A,B TO foo" );
        execute( adminContext, "DENY READ (bar,baz) ON GRAPH * RELATIONSHIPS C,D TO foo" );

        // THEN
        List<String> logLines = readAllLines( logFilename );
        assertThat( logLines, hasSize( 7 ) );
        assertThat( logLines.get( 1 ), containsString( withSubject( adminContext, "DENY MATCH (*) ON GRAPH * ELEMENTS * (*) TO foo" ) ) );
        assertThat( logLines.get( 2 ), containsString( withSubject( adminContext, "DENY MATCH (bar, baz) ON GRAPH * NODES A, B (*) TO foo" ) ) );
        assertThat( logLines.get( 3 ), containsString( withSubject( adminContext, "DENY MATCH (bar, baz) ON GRAPH * RELATIONSHIPS C, D (*) TO foo" ) ) );
        assertThat( logLines.get( 4 ), containsString( withSubject( adminContext, "DENY READ (*) ON GRAPH * ELEMENTS * (*) TO foo" ) ) );
        assertThat( logLines.get( 5 ), containsString( withSubject( adminContext, "DENY READ (bar, baz) ON GRAPH * NODES A, B (*) TO foo" ) ) );
        assertThat( logLines.get( 6 ), containsString( withSubject( adminContext, "DENY READ (bar, baz) ON GRAPH * RELATIONSHIPS C, D (*) TO foo" ) ) );
    }

    @Test
    void shouldLogDenyWrite() throws IOException
    {
        // GIVEN
        execute( adminContext, "CREATE ROLE foo" );

        // WHEN
        execute( adminContext, "DENY WRITE (*) ON GRAPH * TO foo" );

        // THEN
        List<String> logLines = readAllLines( logFilename );
        assertThat( logLines, hasSize( 2 ) );
        assertThat( logLines.get( 1 ), containsString( withSubject( adminContext, "DENY WRITE (*) ON GRAPH * ELEMENTS * (*) TO foo" ) ) );
    }

    @Test
    void shouldLogRevokeTraverse() throws IOException
    {
        // GIVEN
        execute( adminContext, "CREATE ROLE foo" );
        execute( adminContext, "GRANT TRAVERSE ON GRAPH * TO foo" );
        execute( adminContext, "GRANT TRAVERSE ON GRAPH * NODES A,B TO foo" );
        execute( adminContext, "GRANT TRAVERSE ON GRAPH * RELATIONSHIPS C,D TO foo" );

        // WHEN
        execute( adminContext, "REVOKE TRAVERSE ON GRAPH * RELATIONSHIPS C,D FROM foo" );
        execute( adminContext, "REVOKE TRAVERSE ON GRAPH * NODES A,B FROM foo" );
        execute( adminContext, "REVOKE TRAVERSE ON GRAPH * FROM foo" );

        // THEN
        List<String> logLines = readAllLines( logFilename );
        assertThat( logLines, hasSize( 7 ) );
        assertThat( logLines.get( 4 ), containsString( withSubject( adminContext, "REVOKE TRAVERSE ON GRAPH * RELATIONSHIPS C, D (*) FROM foo" ) ) );
        assertThat( logLines.get( 5 ), containsString( withSubject( adminContext, "REVOKE TRAVERSE ON GRAPH * NODES A, B (*) FROM foo" ) ) );
        assertThat( logLines.get( 6 ), containsString( withSubject( adminContext, "REVOKE TRAVERSE ON GRAPH * ELEMENTS * (*) FROM foo" ) ) );
    }

    @Test
    void shouldLogRevokeReadMatch() throws IOException
    {
        // GIVEN
        execute( adminContext, "CREATE ROLE foo" );
        execute( adminContext, "GRANT MATCH (*) ON GRAPH * TO foo" );
        execute( adminContext, "GRANT MATCH (bar,baz) ON GRAPH * NODES A,B TO foo" );
        execute( adminContext, "GRANT MATCH (bar,baz) ON GRAPH * RELATIONSHIPS A,B TO foo" );

        // WHEN
        execute( adminContext, "REVOKE MATCH (bar,baz) ON GRAPH * RELATIONSHIPS A,B FROM foo" );
        execute( adminContext, "REVOKE MATCH (bar,baz) ON GRAPH * NODES A,B FROM foo" );
        execute( adminContext, "REVOKE MATCH (*) ON GRAPH * FROM foo" );

        // GIVEN
        execute( adminContext, "GRANT READ (*) ON GRAPH * TO foo" );
        execute( adminContext, "GRANT READ (bar,baz) ON GRAPH * NODES A,B TO foo" );
        execute( adminContext, "GRANT READ (bar,baz) ON GRAPH * RELATIONSHIPS A,B TO foo" );

        // WHEN
        execute( adminContext, "REVOKE READ (bar,baz) ON GRAPH * RELATIONSHIPS A,B FROM foo" );
        execute( adminContext, "REVOKE READ (bar,baz) ON GRAPH * NODES A,B FROM foo" );
        execute( adminContext, "REVOKE READ (*) ON GRAPH * FROM foo" );

        // THEN
        List<String> logLines = readAllLines( logFilename );
        assertThat( logLines, hasSize( 13 ) );
        assertThat( logLines.get( 4 ), containsString( withSubject( adminContext, "REVOKE MATCH (bar, baz) ON GRAPH * RELATIONSHIPS A, B (*) FROM foo" ) ) );
        assertThat( logLines.get( 5 ), containsString( withSubject( adminContext, "REVOKE MATCH (bar, baz) ON GRAPH * NODES A, B (*) FROM foo" ) ) );
        assertThat( logLines.get( 6 ), containsString( withSubject( adminContext, "REVOKE MATCH (*) ON GRAPH * ELEMENTS * (*) FROM foo" ) ) );
        assertThat( logLines.get( 10 ), containsString( withSubject( adminContext, "REVOKE READ (bar, baz) ON GRAPH * RELATIONSHIPS A, B (*) FROM foo" ) ) );
        assertThat( logLines.get( 11 ), containsString( withSubject( adminContext, "REVOKE READ (bar, baz) ON GRAPH * NODES A, B (*) FROM foo" ) ) );
        assertThat( logLines.get( 12 ), containsString( withSubject( adminContext, "REVOKE READ (*) ON GRAPH * ELEMENTS * (*) FROM foo" ) ) );
    }

    @Test
    void shouldLogRevokeWrite() throws IOException
    {
        // GIVEN
        execute( adminContext, "CREATE ROLE foo" );
        execute( adminContext, "GRANT WRITE (*) ON GRAPH * TO foo" );

        // WHEN
        execute( adminContext, "REVOKE WRITE (*) ON GRAPH * FROM foo" );

        // THEN
        List<String> logLines = readAllLines( logFilename );
        assertThat( logLines, hasSize( 3 ) );
        assertThat( logLines.get( 2 ), containsString( withSubject( adminContext, "REVOKE WRITE (*) ON GRAPH * ELEMENTS * (*) FROM foo" ) ) );
    }

    private List<String> readAllLines( File logFilename ) throws IOException
    {
        FileSystemAbstraction fs = testDirectory.getFileSystem();
        List<String> logLines = new ArrayList<>();
        // this is needed as the EphemeralFSA is broken, and creates a new file when reading a non-existent file from
        // a valid directory
        if ( !fs.fileExists( logFilename ) )
        {
            throw new FileNotFoundException( "File does not exist." );
        }

        try ( BufferedReader reader = new BufferedReader(
                fs.openAsReader( logFilename, StandardCharsets.UTF_8 ) ) )
        {
            for ( String line; (line = reader.readLine()) != null; )
            {
                if ( !line.contains( "Assigned admin role to user 'neo4j'" ) )
                {
                    logLines.add( line );
                }
            }
        }
        return logLines;
    }

    private void execute( LoginContext loginContext, String query )
    {
        execute( loginContext, query, Collections.emptyMap() );
    }

    private void execute( LoginContext loginContext, String query, Map<String,Object> params )
    {
        try ( InternalTransaction transaction = database.beginTransaction( Transaction.Type.explicit, loginContext ) )
        {
            database.execute( query, params );
            transaction.success();
        }
    }

    private String withSubject( LoginContext context, String message )
    {
        return "[" + escape( context.subject().username() ) + "]: " + message;
    }

    private static class StubLoginContext implements LoginContext
    {
        private final AuthSubject subject;
        private final AccessMode accessMode;

        StubLoginContext( String subjectUsername, AccessMode accessMode )
        {
            this.accessMode = accessMode;
            this.subject = new AuthSubject()
            {
                @Override
                public void logout()
                {
                }

                @Override
                public AuthenticationResult getAuthenticationResult()
                {
                    return AuthenticationResult.SUCCESS;
                }

                @Override
                public void setPasswordChangeNoLongerRequired()
                {
                }

                @Override
                public boolean hasUsername( String username )
                {
                    return username.equals( subjectUsername );
                }

                @Override
                public String username()
                {
                    return subjectUsername;
                }
            };
        }

        @Override
        public AuthSubject subject()
        {
            return subject;
        }

        @Override
        public SecurityContext authorize( IdLookup idLookup, String dbName )
        {
            return new SecurityContext( subject, accessMode );
        }
    }
}
