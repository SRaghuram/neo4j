/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import com.neo4j.commercial.edition.factory.CommercialGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.io.File;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.enterprise.auth.InMemoryRoleRepository;
import org.neo4j.server.security.enterprise.auth.SecureHasher;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.server.security.enterprise.log.SecurityLog;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.server.security.auth.BasicAuthManagerTest.password;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;

@ExtendWith( TestDirectoryExtension.class )
class SystemGraphInternalsTest
{
    private GraphDatabaseService database;
    private ContextSwitchingSystemGraphQueryExecutor systemGraphExecutor;
    private SystemGraphRealm realm;

    @Inject
    private TestDirectory testDirectory;

    @BeforeEach
    void setUp() throws Throwable
    {
        CommercialGraphDatabaseFactory factory = new CommercialGraphDatabaseFactory();
        File storeDir = testDirectory.databaseDir();
        final GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( storeDir );
        builder.setConfig( SecuritySettings.auth_provider, SecuritySettings.SYSTEM_GRAPH_REALM_NAME );
        database = builder.newGraphDatabase();
        String activeDbName = ((GraphDatabaseFacade) database).databaseLayout().getDatabaseName();
        DatabaseManager databaseManager = getDatabaseManager();
        systemGraphExecutor = new ContextSwitchingSystemGraphQueryExecutor( databaseManager, activeDbName );
        setupSystemGraphRealm();

        // Neo4j default user should have a DbRole node connected it to admin role already
        assertEquals( 1, nbrOfDbRoleNodes() );
    }

    private void setupSystemGraphRealm() throws Throwable
    {
        AssertableLogProvider log = new AssertableLogProvider();
        SecurityLog securityLog = new SecurityLog( log.getLog( getClass() ) );

        SystemGraphImportOptions importOptions =
                new SystemGraphImportOptions( false, false, false, false, InMemoryUserRepository::new, InMemoryRoleRepository::new, InMemoryUserRepository::new,
                        InMemoryRoleRepository::new, InMemoryUserRepository::new, InMemoryUserRepository::new );

        SecureHasher secureHasher = new SecureHasher();
        SystemGraphOperations systemGraphOperations = new SystemGraphOperations( systemGraphExecutor, secureHasher );
        realm = new SystemGraphRealm(
                systemGraphOperations,
                new SystemGraphInitializer( systemGraphExecutor, systemGraphOperations, importOptions, secureHasher, securityLog ),
                true,
                new SecureHasher(),
                new BasicPasswordPolicy(),
                Mockito.mock( AuthenticationStrategy.class ),
                true,
                true
        );
        realm.initialize();
        realm.start(); // creates default user and roles
    }

    @AfterEach
    void tearDown()
    {
        if ( database != null )
        {
            database.shutdown();
            database = null;
        }
    }

    private DatabaseManager getDatabaseManager()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }

    @Test
    void shouldNotShareDbRoleNodeBetweenUsersWithSameRole() throws Exception
    {
        setupTwoReaders();
        assertEquals( 3, nbrOfDbRoleNodes() );
    }

    @Test
    void shouldRemoveDbRoleNodeWhenUserIsDeleted() throws Exception
    {
        setupTwoReaders();

        realm.deleteUser( "Neo" );
        assertEquals( 2, nbrOfDbRoleNodes() );

        realm.deleteUser( "Trinity" );
        assertEquals( 1,    nbrOfDbRoleNodes() );
    }

    @Test
    void shouldRemoveDbRoleNodeWhenUserIsUnassigned() throws Exception
    {
       setupTwoReaders();

       realm.removeRoleFromUser( READER, "Neo" );
       assertEquals( 2, nbrOfDbRoleNodes() );

       realm.removeRoleFromUser( READER, "Trinity" );
       assertEquals( 1, nbrOfDbRoleNodes() );
    }

    @Test
    void shouldRemoveDbRoleNodeWhenDeletingRole() throws Exception
    {
        realm.newRole( "tmpRole", "neo4j" );
        assertEquals( 2, nbrOfDbRoleNodes() );

        realm.deleteRole( "tmpRole" );
        assertEquals( 1, nbrOfDbRoleNodes() );
    }

    private void setupTwoReaders() throws InvalidArgumentsException
    {
        realm.newUser( "Neo", password( "abc" ), false );
        realm.newUser( "Trinity", password( "123" ), false );
        realm.addRoleToUser( READER, "Neo" );
        realm.addRoleToUser( READER, "Trinity" );
    }

    private long nbrOfDbRoleNodes()
    {
        String query = "MATCH (dbr:DbRole) RETURN count(dbr)";
        return systemGraphExecutor.executeQueryLong( query );
    }
}
