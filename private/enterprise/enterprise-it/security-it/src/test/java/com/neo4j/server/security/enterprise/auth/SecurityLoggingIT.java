/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.apache.commons.io.IOUtils.readLines;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;

@TestDirectoryExtension
class SecurityLoggingIT
{
    @Inject
    public TestDirectory testDirectory;

    private Path securityLog;
    private FileSystemAbstraction fileSystem;
    private EnterpriseAuthManager authManager;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setup()
    {
        Path homeDir = testDirectory.directoryPath( "logs" );
        securityLog = homeDir.resolve( "security.log" );

        managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() )
                .setConfig( GraphDatabaseSettings.auth_enabled, true )
                .setConfig( GraphDatabaseSettings.logs_directory, homeDir.toAbsolutePath() ).build();

        var db = (GraphDatabaseFacade) managementService.database( DEFAULT_DATABASE_NAME );
        authManager = db.getDependencyResolver().resolveDependency( EnterpriseAuthManager.class );
        fileSystem = db.getDependencyResolver().resolveDependency( FileSystemAbstraction.class );
    }

    @Test
    void shouldLogAuthentication() throws Exception
    {
        // GIVEN
        executeSystemCommand( "CREATE USER mats SET PASSWORD 'neo4j' CHANGE NOT REQUIRED" );

        // WHEN
        authManager.login( authToken( "mats", "neo4j" ) );

        // flush log
        managementService.shutdown();

        // THEN
        SecurityLog log = new SecurityLog();
        log.load();
        log.assertHasLine( "mats", "logged in" );
    }

    @Test
    void shouldLogAuthenticationPasswordChangeRequired() throws Exception
    {
        // GIVEN
        executeSystemCommand( "CREATE USER mats SET PASSWORD 'neo4j' CHANGE REQUIRED" );

        // WHEN
        authManager.login( authToken( "mats", "neo4j" ) );

        // flush log
        managementService.shutdown();

        // THEN
        SecurityLog log = new SecurityLog();
        log.load();
        log.assertHasLine( "mats", "logged in (password change required)" );
    }

    @Test
    void shouldLogFailedAuthentication() throws Exception
    {
        // WHEN
        authManager.login( authToken( "mats", "neo4j" ) );

        // flush log
        managementService.shutdown();

        // THEN
        SecurityLog log = new SecurityLog();
        log.load();
        log.assertHasLine( "mats", "failed to log in" );
    }

    @Test
    void shouldLogSecurityCommands() throws Exception
    {
        // WHEN
        executeSystemCommand( "CREATE USER mats SET PASSWORD 'neo4j' CHANGE NOT REQUIRED" );
        executeSystemCommand( "CREATE ROLE role1" );
        executeSystemCommand( "CALL dbms.security.createRole('role2')" );

        // flush log
        managementService.shutdown();

        // THEN
        SecurityLog log = new SecurityLog();
        log.load();
        log.assertHasLine( "", "CREATE USER mats SET PASSWORD '******' CHANGE NOT REQUIRED" );
        log.assertHasLine( "", "CREATE ROLE role1" );
        log.assertHasLine( "", "CREATE ROLE role2" );
    }

    private void executeSystemCommand( String call )
    {
        var gdb = (GraphDatabaseFacade) managementService.database( SYSTEM_DATABASE_NAME );
        try ( InternalTransaction tx = gdb.beginTransaction( KernelTransaction.Type.EXPLICIT, EnterpriseLoginContext.AUTH_DISABLED ) )
        {
            var result = tx.execute( call, Collections.emptyMap() );
            result.accept( r -> true );
            tx.commit();
        }
    }

    private class SecurityLog
    {
        List<String> lines;

        void load() throws IOException
        {
            try ( var reader = fileSystem.openAsReader( securityLog, StandardCharsets.UTF_8 ) )
            {
                lines = readLines( reader );
            }
        }

        void assertHasLine( String subject, String msg )
        {
            Objects.requireNonNull( lines );
            assertThat( lines, hasItem( containsString( "[" + subject + "]: " + msg ) ) );
        }
    }
}
