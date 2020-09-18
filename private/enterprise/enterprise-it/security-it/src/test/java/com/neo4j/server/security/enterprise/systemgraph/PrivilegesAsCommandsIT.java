/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

class PrivilegesAsCommandsIT
{
    DatabaseManagementService dbms = new TestEnterpriseDatabaseManagementServiceBuilder().impermanent().build();
    GraphDatabaseAPI gdb = (GraphDatabaseAPI) dbms.database(SYSTEM_DATABASE_NAME);

    @Test
    void shouldReplicateInitialState()
    {
        // GIVEN
        GraphDatabaseService system = dbms.database( SYSTEM_DATABASE_NAME );

        // WHEN
        var commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true);

        // THEN
        assertThat( commands.roleSetup ).isNotEmpty();
        assertThat( commands.userSetup ).isNotEmpty();
        assertRecreatesOriginal( commands, DEFAULT_DATABASE_NAME, true );
    }

    @Test
    void shouldGetNoPrivilegesWithNoGrantOnRequestedDatabase()
    {
        // GIVEN
        dbms.createDatabase( "graph.db" );
        GraphDatabaseService system = getCleanSystemDB();
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT ACCESS ON DATABASE $db to role", Map.of( "db", DEFAULT_DATABASE_NAME ) );
            tx.commit();
        }

        // WHEN
        var commands = getBackupCommands( system, "graph.db", true, true);

        // THEN
        assertThat( commands.roleSetup ).filteredOn( c -> !c.startsWith( "CREATE DATABASE" ) ).isEmpty();
        assertThat( commands.userSetup ).isEmpty();
    }

    @Test
    void shouldSavePrivilegeOnDefaultDatabase()
    {
        // GIVEN
        GraphDatabaseService system = getCleanSystemDB();
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT ACCESS ON DEFAULT DATABASE TO role" );
            tx.commit();
        }

        // WHEN
        var commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true);

        // THEN
        assertThat( commands.roleSetup ).containsExactly(
                "CREATE ROLE `role` IF NOT EXISTS",
                "GRANT ACCESS ON DATABASE $database TO `role`"
        );
        assertThat( commands.userSetup ).isEmpty();
        assertRecreatesOriginal( commands, DEFAULT_DATABASE_NAME, true );
    }

    //system database

    @Test
    void shouldSaveDenies()
    {
        // GIVEN
        GraphDatabaseService system = getCleanSystemDB();
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE ROLE role" );
            tx.execute( "DENY TRAVERSE ON GRAPH * NODES A TO role" );
            tx.execute( "DENY START ON DATABASE neo4j TO role" );
            tx.commit();
        }

        // WHEN
        var commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true);

        // THEN
        assertThat( commands.roleSetup ).hasSize( 3 );
        assertThat( commands.userSetup ).isEmpty();
        assertRecreatesOriginal( commands, DEFAULT_DATABASE_NAME, true );
    }

    @Test
    void shouldNotSaveUserWithNoRelevantRole()
    {
        // GIVEN
        GraphDatabaseService system = getCleanSystemDB();
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE USER user SET PASSWORD 'abc123'" );
            tx.execute( "CREATE ROLE role" );
            tx.commit();
        }

        // WHEN
        var commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true);

        // THEN
        assertThat( commands.roleSetup ).isEmpty();
        assertThat( commands.userSetup ).isEmpty();
    }

    @Test
    void shouldSaveUserWithRelevantRole()
    {
        // GIVEN
        GraphDatabaseService system = getCleanSystemDB();
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT TRAVERSE ON GRAPH * NODES * TO role" );
            tx.execute( "CREATE USER user SET PASSWORD 'abc123'" );
            tx.execute( "GRANT ROLE role TO user" );
            tx.commit();
        }

        // WHEN
        var commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true);

        // THEN
        assertThat( commands.roleSetup ).hasSize(2);
        assertThat( commands.userSetup ).hasSize(2);
        assertRecreatesOriginal( commands, DEFAULT_DATABASE_NAME, true );
    }

    @Test
    void shouldNotSaveDuplicateUserWhenAssignedMultipleRoles()
    {
        // GIVEN
        GraphDatabaseService system = getCleanSystemDB();
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE ROLE foo" );
            tx.execute( "GRANT TRAVERSE ON GRAPH * NODES * TO foo" );
            tx.execute( "CREATE ROLE bar" );

            tx.execute( "CREATE USER user SET PASSWORD 'abc123'" );
            tx.execute( "GRANT ROLE foo TO user" );
            tx.execute( "GRANT ROLE bar TO user" );
            tx.commit();
        }

        var commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true);
        assertThat( commands.userSetup ).filteredOn( s -> s.startsWith( "CREATE USER" ) ).hasSize( 1 );
        assertRecreatesOriginal( commands, DEFAULT_DATABASE_NAME, true );
    }

    @Test
    void shouldNotSaveUsersWhenToldNotTo()
    {
        // GIVEN
        GraphDatabaseService system = getCleanSystemDB();
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT TRAVERSE ON GRAPH * NODES * TO role" );
            tx.execute( "CREATE USER foo SET PASSWORD 'abc123'" );
            tx.execute( "CREATE USER bar SET PASSWORD 'abc123'" );

            tx.execute( "GRANT ROLE role TO foo" );
            tx.execute( "GRANT ROLE role TO bar" );
            tx.commit();
        }

        var commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, false, true );
        assertThat( commands.userSetup).isEmpty();
        assertRecreatesOriginal( commands, DEFAULT_DATABASE_NAME, true );
    }

    @Test
    void shouldNotSaveRolesWhenToldNotTo()
    {
        // GIVEN
        GraphDatabaseService system = dbms.database( SYSTEM_DATABASE_NAME );
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT TRAVERSE ON GRAPH * NODES * TO role" );
            tx.execute( "CREATE USER foo SET PASSWORD 'abc123'" );
            tx.execute( "CREATE USER bar SET PASSWORD 'abc123'" );

            tx.execute( "GRANT ROLE role TO foo" );
            tx.execute( "GRANT ROLE role TO bar" );
            tx.commit();
        }

        var commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, false );
        assertThat( commands.roleSetup).isEmpty();
        assertRecreatesOriginal( commands, DEFAULT_DATABASE_NAME, false );
    }

    @Test
    void shouldNotSaveUsersOrRolesWhenToldNotTo()
    {
        // GIVEN
        GraphDatabaseService system = dbms.database( SYSTEM_DATABASE_NAME );
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT TRAVERSE ON GRAPH * NODES * TO role" );
            tx.execute( "CREATE USER foo SET PASSWORD 'abc123'" );
            tx.execute( "CREATE USER bar SET PASSWORD 'abc123'" );

            tx.execute( "GRANT ROLE role TO foo" );
            tx.execute( "GRANT ROLE role TO bar" );
            tx.commit();
        }

        var commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, false, false );
        assertThat( commands.roleSetup).isEmpty();
        assertRecreatesOriginal( commands, DEFAULT_DATABASE_NAME, false );
    }

    @Test
    void shouldRecreateDatabase()
    {
        // GIVEN
        GraphDatabaseService system = getCleanSystemDB();
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE DATABASE newDb" );
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT TRAVERSE ON GRAPH newDb NODES * TO role" );
            tx.execute( "CREATE USER foo SET PASSWORD 'abc123'" );
            tx.execute( "GRANT ROLE role TO foo" );
            tx.commit();
        }

        var commands = getBackupCommands( system, "newDb", true, true );
        assertThat( commands.roleSetup ).filteredOn( c -> c.startsWith( "CREATE DATABASE" ) ).hasSize( 1 );
        assertThat( commands.roleSetup ).filteredOn( c -> c.startsWith( "STOP DATABASE" ) ).hasSize( 0 );
        assertRecreatesOriginal( commands, "newDb", true);
    }

    @Test
    void shouldRecreateOfflineDatabaseAsOffline()
    {
        // GIVEN
        GraphDatabaseService system = getCleanSystemDB();
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE DATABASE newDb" );
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT TRAVERSE ON GRAPH newDb NODES * TO role" );
            tx.execute( "CREATE USER foo SET PASSWORD 'abc123'" );
            tx.execute( "GRANT ROLE role TO foo" );
            tx.execute( "STOP DATABASE newDb" );
            tx.commit();
        }

        var commands = getBackupCommands( system, "newDb", true, true );
        assertThat( commands.roleSetup ).filteredOn( c -> c.startsWith( "STOP DATABASE" ) ).hasSize( 1 );
        assertRecreatesOriginal( commands, "newDb", true);
    }

    @Test
    void shouldNotSaveDbmsPrivileges()
    {
        // GIVEN
        GraphDatabaseService system = getCleanSystemDB();
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT EXECUTE PROCEDURE * ON DBMS TO role" );
            tx.execute( "GRANT CREATE ROLE ON DBMS TO role" );
            tx.execute( "GRANT ALL ON DBMS TO role" );
            tx.commit();
        }

        // WHEN
        var commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true);

        // THEN
        assertThat( commands.roleSetup ).isEmpty();
        assertThat( commands.userSetup ).isEmpty();
    }

    /**
     * Get a clean system database, only keeping Version node, Database nodes and PUBLIC role.
     */
    private GraphDatabaseService getCleanSystemDB()
    {
        GraphDatabaseService system = dbms.database( SYSTEM_DATABASE_NAME );
        try ( Transaction tx = system.beginTx() )
        {
            for ( Node node : tx.getAllNodes() )
            {
                for ( Relationship rel : node.getRelationships() )
                {
                    rel.delete();
                }
                if ( !(node.hasLabel( Label.label( "Database" )) && node.getProperty( "name" ).equals( DEFAULT_DATABASE_NAME )) &&
                     !(node.hasLabel( Label.label( "Database" )) && node.getProperty( "name" ).equals( SYSTEM_DATABASE_NAME )) &&
                     !node.hasLabel( Label.label( "Version" ) ) &&
                     !(node.hasLabel( Label.label( "Role" ) ) && node.getProperty( "name" ).equals( PUBLIC ) ) )
                {
                    node.delete();
                }
            }
            tx.commit();
        }
        return system;
    }

    private BackupCommands getBackupCommands( GraphDatabaseService system, String databaseName, boolean saveUsers, boolean saveRoles )
    {
        var component = gdb.getDependencyResolver().resolveDependency( EnterpriseSecurityGraphComponent.class );
        try ( Transaction tx = system.beginTx() )
        {
            return component.getBackupCommands( tx, databaseName, saveUsers, saveRoles );
        }
    }

    private void assertRecreatesOriginal( BackupCommands backup, String databaseName, boolean saveRoles )
    {
        GraphDatabaseService system = getCleanSystemDB();
        try ( Transaction tx = system.beginTx() )
        {
            for ( String roleCommand : backup.roleSetup )
            {
                tx.execute( roleCommand, Map.of("database", databaseName) );
            }
            for ( String userCommand : backup.userSetup )
            {
                tx.execute( userCommand );
            }
            tx.commit();
        }
        BackupCommands newBackup = getBackupCommands( system, databaseName, true, true);
        assertThat( newBackup.roleSetup ).containsExactlyInAnyOrderElementsOf( backup.roleSetup );

        if ( saveRoles ) {
            assertThat( newBackup.userSetup ).containsExactlyInAnyOrderElementsOf( backup.userSetup );
        } else {
            assertThat( newBackup.userSetup ).isEmpty();
        }
    }

}