/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.LabelSegment;
import com.neo4j.server.security.enterprise.auth.RelTypeSegment;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.ProcedureSegment;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.internal.kernel.api.security.UserSegment;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ACCESS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CONSTRAINT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DATABASE_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DBMS_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE_ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE_BOOSTED;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.GRAPH_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.INDEX;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.MATCH;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.MERGE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.READ;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.REMOVE_LABEL;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SET_LABEL;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SET_PROPERTY;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.START_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.STOP_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TOKEN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TRANSACTION_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TRAVERSE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.WRITE;

class PrivilegesAsCommandsIT
{
    static final String DB_PARAM = "database";
    static DatabaseManagementService dbms;
    GraphDatabaseAPI system;

    @BeforeAll
    static void setup()
    {
        dbms = new TestEnterpriseDatabaseManagementServiceBuilder().impermanent().build();
    }

    @AfterAll
    static void teardown()
    {
        dbms.shutdown();
    }

    @BeforeEach
    void resetSystemDb()
    {
        system = getCleanSystemDB();
    }

    @Test
    void shouldConstructValidPrivileges() throws InvalidArgumentsException
    {
        List<String> commands = new ArrayList<>();
        String role = "role";
        String user = "user";

        for ( ResourcePrivilege.GrantOrDeny privilegeType : ResourcePrivilege.GrantOrDeny.values() )
        {
            for ( PrivilegeAction action : PrivilegeAction.values() )
            {
                if ( ACCESS.satisfies( action ) )
                {
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.DatabaseResource(), Segment.ALL ) );
                }
                else if ( TRAVERSE.satisfies( action ) )
                {
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.GraphResource(), LabelSegment.ALL ) );
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.GraphResource(), RelTypeSegment.ALL ) );
                }
                else if ( READ.satisfies( action ) )
                {
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.AllPropertiesResource(), LabelSegment.ALL ) );
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.AllPropertiesResource(), RelTypeSegment.ALL ) );
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.PropertyResource( "foo" ), LabelSegment.ALL ) );
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.PropertyResource( "foo" ), RelTypeSegment.ALL ) );
                }
                else if ( MATCH.satisfies( action ) )
                {
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.AllPropertiesResource(), LabelSegment.ALL ) );
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.AllPropertiesResource(), RelTypeSegment.ALL ) );
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.PropertyResource( "foo" ), LabelSegment.ALL ) );
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.PropertyResource( "foo" ), RelTypeSegment.ALL ) );
                }
                else if ( SET_LABEL.satisfies( action ) )
                {
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.AllLabelsResource(), LabelSegment.ALL ) );
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.LabelResource( "foo" ), LabelSegment.ALL ) );
                }
                else if ( REMOVE_LABEL.satisfies( action ) )
                {
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.AllLabelsResource(), LabelSegment.ALL ) );
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.LabelResource( "foo" ), LabelSegment.ALL ) );
                }
                else if ( SET_PROPERTY.satisfies( action ) )
                {
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.AllPropertiesResource(), LabelSegment.ALL ) );
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.AllPropertiesResource(), RelTypeSegment.ALL ) );
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.PropertyResource( "foo" ), LabelSegment.ALL ) );
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.PropertyResource( "foo" ), RelTypeSegment.ALL ) );
                }
                else if ( WRITE.satisfies( action ) )
                {
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.GraphResource(), LabelSegment.ALL ) );
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.GraphResource(), RelTypeSegment.ALL ) );
                }
                else if ( MERGE.satisfies( action ) )
                {
                    if ( privilegeType == ResourcePrivilege.GrantOrDeny.DENY )
                    {
                        // not supported
                    }
                    else
                    {
                        commands.addAll( getGrantFor( role, privilegeType, action, new Resource.AllPropertiesResource(), LabelSegment.ALL ) );
                        commands.addAll( getGrantFor( role, privilegeType, action, new Resource.AllPropertiesResource(), RelTypeSegment.ALL ) );
                        commands.addAll( getGrantFor( role, privilegeType, action, new Resource.PropertyResource( "foo" ), LabelSegment.ALL ) );
                        commands.addAll( getGrantFor( role, privilegeType, action, new Resource.PropertyResource( "foo" ), RelTypeSegment.ALL ) );
                    }
                }
                else if ( TRANSACTION_MANAGEMENT.satisfies( action ) )
                {
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.DatabaseResource(), new UserSegment( user ) ) );
                }
                else if ( DBMS_ACTIONS.satisfies( action ) ||
                          START_DATABASE.satisfies( action ) ||
                          STOP_DATABASE.satisfies( action ) || action == ADMIN )
                {
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.DatabaseResource(), Segment.ALL ) );
                }
                else if ( INDEX.satisfies( action ) )
                {
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.DatabaseResource(), Segment.ALL ) );
                }
                else if ( CONSTRAINT.satisfies( action ) )
                {
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.DatabaseResource(), Segment.ALL ) );
                }
                else if ( TOKEN.satisfies( action ) )
                {
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.DatabaseResource(), Segment.ALL ) );
                }
                else if ( EXECUTE.satisfies( action ) )
                {
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.DatabaseResource(), ProcedureSegment.ALL ) );
                }
                else if ( EXECUTE_BOOSTED.satisfies( action ) )
                {
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.DatabaseResource(), ProcedureSegment.ALL ) );
                }
                else if ( EXECUTE_ADMIN.satisfies( action ) )
                {
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.DatabaseResource(), ProcedureSegment.ALL ) );
                }
                else if ( GRAPH_ACTIONS.satisfies( action ) )
                {
                    // grouping of other privileges that are already tested
                }
                else if ( DATABASE_ACTIONS.satisfies( action ) )
                {
                    commands.addAll( getGrantFor( role, privilegeType, action, new Resource.DatabaseResource(), Segment.ALL ) );
                }
                else
                {
                    fail( "Unhandled PrivilegeAction: " + action );
                }
            }
        }

        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( String.format( "CREATE ROLE %s", role ) );
            tx.execute( String.format( "CREATE USER %s SET PASSWORD 'abc123'", user ) );
            tx.execute( String.format( "GRANT ROLE %s TO %s", role, user ) );
            // all commands should be valid Cypher syntax
            commands.forEach( command -> tx.execute( command, Map.of( DB_PARAM, DEFAULT_DATABASE_NAME ) ) );
            tx.commit();
        }

        BackupCommands backupCommands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true );
        assertThat( backupCommands.roleSetup ).filteredOn( c -> !c.startsWith( "CREATE" ) )
                                              .containsExactlyInAnyOrderElementsOf( commands );
        assertThat( backupCommands.userSetup ).hasSize( 2 );
    }

    @Test
    void shouldReplicateInitialState()
    {
        // GIVEN
        DatabaseManagementService dbms = new TestEnterpriseDatabaseManagementServiceBuilder().impermanent().build();
        GraphDatabaseAPI system = (GraphDatabaseAPI) dbms.database( SYSTEM_DATABASE_NAME );

        // WHEN
        BackupCommands commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true );

        // THEN
        assertThat( commands.roleSetup ).isNotEmpty();
        assertThat( commands.userSetup ).isNotEmpty();
        assertRecreatesOriginal( commands, DEFAULT_DATABASE_NAME, true );

        dbms.shutdown();
    }

    @Test
    void shouldGetNoPrivilegesWithoutGrantOnRequestedDatabase()
    {
        // GIVEN
        dbms.createDatabase( "graph.db" );
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT ACCESS ON DATABASE $db to role", Map.of( "db", DEFAULT_DATABASE_NAME ) );
            tx.execute( "CREATE USER user SET PASSWORD 'abc123'" );
            tx.execute( "GRANT ROLE role TO user" );
            tx.commit();
        }

        // WHEN
        BackupCommands commands = getBackupCommands( system, "graph.db", true, true );

        // THEN
        assertThat( commands.roleSetup ).containsExactlyInAnyOrder(
                String.format( "CREATE DATABASE $%s IF NOT EXISTS", DB_PARAM )
        );
        assertThat( commands.userSetup ).isEmpty();
    }

    @Test
    void shouldSavePrivilegeOnDefaultDatabase()
    {
        // GIVEN
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT ACCESS ON DEFAULT DATABASE TO role" );
            tx.commit();
        }

        // WHEN
        BackupCommands commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true );

        // THEN
        assertThat( commands.roleSetup ).containsExactlyInAnyOrder(
                String.format( "CREATE DATABASE $%s IF NOT EXISTS", DB_PARAM ),
                "CREATE ROLE `role` IF NOT EXISTS",
                String.format( "GRANT ACCESS ON DATABASE $%s TO `role`", DB_PARAM )
        );
        assertThat( commands.userSetup ).isEmpty();
        assertRecreatesOriginal( commands, DEFAULT_DATABASE_NAME, true );
    }

    @Test
    void shouldSaveDenies()
    {
        // GIVEN
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE ROLE role" );
            tx.execute( "DENY TRAVERSE ON GRAPH * NODES A TO role" );
            tx.execute( "DENY START ON DATABASE neo4j TO role" );
            tx.commit();
        }

        // WHEN
        BackupCommands commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true );

        // THEN
        assertThat( commands.roleSetup ).containsExactlyInAnyOrder(
                String.format( "CREATE DATABASE $%s IF NOT EXISTS", DB_PARAM ),
                "CREATE ROLE `role` IF NOT EXISTS",
                String.format( "DENY TRAVERSE ON GRAPH $%s NODE A TO `role`", DB_PARAM ),
                String.format( "DENY START ON DATABASE $%s TO `role`", DB_PARAM )
        );
        assertThat( commands.userSetup ).isEmpty();
        assertRecreatesOriginal( commands, DEFAULT_DATABASE_NAME, true );
    }

    @Test
    void shouldNotSaveUserWithNoRelevantRole()
    {
        // GIVEN
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE USER user SET PASSWORD 'abc123'" );
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT ROLE role TO user" );
            tx.commit();
        }

        // WHEN
        BackupCommands commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true );

        // THEN
        assertThat( commands.roleSetup ).containsExactlyInAnyOrder(
                String.format( "CREATE DATABASE $%s IF NOT EXISTS", DB_PARAM )
        );
        assertThat( commands.userSetup ).isEmpty();
    }

    @Test
    void shouldSaveUserWithRelevantRole()
    {
        // GIVEN
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT TRAVERSE ON DEFAULT GRAPH NODES * TO role" );
            tx.execute( "CREATE USER user SET PASSWORD 'abc123'" );
            tx.execute( "GRANT ROLE role TO user" );
            tx.commit();
        }

        // WHEN
        BackupCommands commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true );

        // THEN
        assertThat( commands.roleSetup ).containsExactlyInAnyOrder(
                String.format( "CREATE DATABASE $%s IF NOT EXISTS", DB_PARAM ),
                "CREATE ROLE `role` IF NOT EXISTS",
                String.format( "GRANT TRAVERSE ON GRAPH $%s NODE * TO `role`", DB_PARAM )
        );
        assertThat( commands.userSetup ).filteredOn( c -> c.startsWith( "CREATE USER `user` IF NOT EXISTS SET ENCRYPTED PASSWORD" ) ).hasSize( 1 );
        assertThat( commands.userSetup ).contains( "GRANT ROLE `role` TO `user`" );
        assertThat( commands.userSetup ).hasSize( 2 );
        assertRecreatesOriginal( commands, DEFAULT_DATABASE_NAME, true );
    }

    @Test
    void shouldSaveAnyRoleConfiguration()
    {
        // GIVEN
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT ALL GRAPH PRIVILEGES ON GRAPH * TO role" );
            tx.execute( "CREATE USER user1 SET PASSWORD 'abc123' CHANGE REQUIRED" );
            tx.execute( "CREATE USER user2 SET PASSWORD 'abc123' CHANGE NOT REQUIRED" );
            tx.execute( "CREATE USER user3 SET PASSWORD 'abc123' SET STATUS ACTIVE" );
            tx.execute( "CREATE USER user4 SET PASSWORD 'abc123' SET STATUS SUSPENDED" );
            tx.execute( "CREATE USER user5 SET PASSWORD 'abc123' CHANGE REQUIRED SET STATUS ACTIVE" );
            tx.execute( "CREATE USER user6 SET PASSWORD 'abc123' CHANGE NOT REQUIRED SET STATUS SUSPENDED" );
            tx.execute( "GRANT ROLE role TO user1" );
            tx.execute( "GRANT ROLE role TO user2" );
            tx.execute( "GRANT ROLE role TO user3" );
            tx.execute( "GRANT ROLE role TO user4" );
            tx.execute( "GRANT ROLE role TO user5" );
            tx.execute( "GRANT ROLE role TO user6" );
            tx.commit();
        }

        // WHEN
        BackupCommands commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true );

        // THEN
        assertThat( commands.userSetup ).filteredOn( c -> c.contains( "CHANGE REQUIRED" ) ).hasSize( 4 );
        assertThat( commands.userSetup ).filteredOn( c -> c.contains( "CHANGE NOT REQUIRED" ) ).hasSize( 2 );
        assertThat( commands.userSetup ).filteredOn( c -> c.contains( "SET STATUS ACTIVE" ) ).hasSize( 4 );
        assertThat( commands.userSetup ).filteredOn( c -> c.contains( "SET STATUS SUSPENDED" ) ).hasSize( 2 );
        assertRecreatesOriginal( commands, DEFAULT_DATABASE_NAME, true );
    }

    @Test
    void shouldOnlySaveRelevantRoles()
    {
        // GIVEN
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE ROLE foo" );
            tx.execute( "GRANT ALL GRAPH PRIVILEGES ON GRAPH * TO foo" );
            tx.execute( "CREATE ROLE bar" );
            tx.execute( "CREATE USER user SET PASSWORD 'abc123'" );
            tx.execute( "GRANT ROLE foo TO user" );
            tx.execute( "GRANT ROLE bar TO user" );
            tx.commit();
        }

        // WHEN
        BackupCommands commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true );

        // THEN
        assertThat( commands.roleSetup ).containsExactlyInAnyOrder(
                String.format( "CREATE DATABASE $%s IF NOT EXISTS", DB_PARAM ),
                "CREATE ROLE `foo` IF NOT EXISTS",
                String.format( "GRANT ALL GRAPH PRIVILEGES ON GRAPH $%s TO `foo`", DB_PARAM )

        );
        assertThat( commands.userSetup ).filteredOn( c -> c.startsWith( "CREATE USER `user` IF NOT EXISTS SET ENCRYPTED PASSWORD" ) ).hasSize( 1 );
        assertThat( commands.userSetup ).contains( "GRANT ROLE `foo` TO `user`" );
        assertThat( commands.userSetup ).hasSize( 2 );
        assertRecreatesOriginal( commands, DEFAULT_DATABASE_NAME, true );
    }

    @Test
    void shouldNotSaveUsersWhenToldNotTo()
    {
        // GIVEN
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT CREATE INDEX ON DEFAULT DATABASE TO role" );
            tx.execute( "CREATE USER foo SET PASSWORD 'abc123'" );
            tx.execute( "CREATE USER bar SET PASSWORD 'abc123'" );

            tx.execute( "GRANT ROLE role TO foo" );
            tx.execute( "GRANT ROLE role TO bar" );
            tx.commit();
        }

        // WHEN
        BackupCommands commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, false, true );

        // THEN
        assertThat( commands.roleSetup ).containsExactlyInAnyOrder(
                String.format( "CREATE DATABASE $%s IF NOT EXISTS", DB_PARAM ),
                "CREATE ROLE `role` IF NOT EXISTS",
                String.format( "GRANT CREATE INDEX ON DATABASE $%s TO `role`", DB_PARAM )
        );
        assertThat( commands.userSetup ).isEmpty();
        assertRecreatesOriginal( commands, DEFAULT_DATABASE_NAME, true );
    }

    @Test
    void shouldNotSaveRolesWhenToldNotTo()
    {
        // GIVEN
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT WRITE ON DEFAULT GRAPH TO role" );
            tx.execute( "CREATE USER foo SET PASSWORD 'abc123'" );
            tx.execute( "CREATE USER bar SET PASSWORD 'abc123'" );

            tx.execute( "GRANT ROLE role TO foo" );
            tx.execute( "GRANT ROLE role TO bar" );
            tx.commit();
        }

        // WHEN
        BackupCommands commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, false );

        // THEN
        assertThat( commands.roleSetup ).containsExactlyInAnyOrder(
                String.format( "CREATE DATABASE $%s IF NOT EXISTS", DB_PARAM )
        );
        assertThat( commands.userSetup ).filteredOn( c -> c.startsWith( "CREATE USER `foo` IF NOT EXISTS SET ENCRYPTED PASSWORD" ) ).hasSize( 1 );
        assertThat( commands.userSetup ).filteredOn( c -> c.startsWith( "CREATE USER `bar` IF NOT EXISTS SET ENCRYPTED PASSWORD" ) ).hasSize( 1 );
        assertThat( commands.userSetup ).hasSize( 2 );
        assertRecreatesOriginal( commands, DEFAULT_DATABASE_NAME, false );
    }

    @Test
    void shouldNotSaveUsersOrRolesWhenToldNotTo()
    {
        // GIVEN
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT START ON DATABASE * TO role" );
            tx.execute( "CREATE USER foo SET PASSWORD 'abc123'" );
            tx.execute( "CREATE USER bar SET PASSWORD 'abc123'" );

            tx.execute( "GRANT ROLE role TO foo" );
            tx.execute( "GRANT ROLE role TO bar" );
            tx.commit();
        }

        // WHEN
        BackupCommands commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, false, false );

        // THEN
        assertThat( commands.roleSetup ).containsExactlyInAnyOrder(
                String.format( "CREATE DATABASE $%s IF NOT EXISTS", DB_PARAM )
        );
        assertThat( commands.userSetup ).isEmpty();
        assertRecreatesOriginal( commands, DEFAULT_DATABASE_NAME, false );
    }

    @Test
    void shouldRecreateDatabase()
    {
        // GIVEN
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE DATABASE newDb" );
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT ACCESS ON DATABASE * TO role" );
            tx.execute( "CREATE USER foo SET PASSWORD 'abc123'" );
            tx.execute( "GRANT ROLE role TO foo" );
            tx.commit();
        }

        // WHEN
        BackupCommands commands = getBackupCommands( system, "newDb", true, true );

        // THEN
        assertThat( commands.roleSetup ).containsExactlyInAnyOrder(
                String.format( "CREATE DATABASE $%s IF NOT EXISTS", DB_PARAM ),
                "CREATE ROLE `role` IF NOT EXISTS",
                String.format( "GRANT ACCESS ON DATABASE $%s TO `role`", DB_PARAM )
        );
        assertRecreatesOriginal( commands, "newDb", true );
    }

    @Test
    void shouldRecreateOfflineDatabaseAsOffline()
    {
        // GIVEN
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE DATABASE newDb" );
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT MERGE {*} ON GRAPH newDb NODES * TO role" );
            tx.execute( "CREATE USER foo SET PASSWORD 'abc123'" );
            tx.execute( "GRANT ROLE role TO foo" );
            tx.execute( "STOP DATABASE newDb" );
            tx.commit();
        }

        BackupCommands commands = getBackupCommands( system, "newDb", true, true );
        assertThat( commands.roleSetup ).filteredOn( c -> c.startsWith( "CREATE DATABASE" ) ).hasSize( 1 );
        assertThat( commands.roleSetup ).filteredOn( c -> c.startsWith( "STOP DATABASE" ) ).hasSize( 1 );
        assertRecreatesOriginal( commands, "newDb", true );
    }

    @Test
    void shouldRestoreWithDbNameConflict()
    {
        // GIVEN
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE DATABASE database" );
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT READ {*} ON GRAPH database NODES * TO role" );
            tx.execute( "CREATE USER foo SET PASSWORD 'password'" );
            tx.execute( "GRANT ROLE role TO foo" );
            tx.commit();
        }

        BackupCommands commands = getBackupCommands( system, "database", true, true );

        GraphDatabaseAPI newSystem = getCleanSystemDB();

        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE DATABASE database" );

            // WHEN
            commands.roleSetup.forEach( c -> tx.execute( c, Map.of( "database", "database" ) ) );
            commands.userSetup.forEach( tx::execute );
            tx.commit();
        }
        var newCommands = getBackupCommands( newSystem, "database", true, true );

        // THEN
        assertThat( newCommands.roleSetup ).containsExactlyInAnyOrderElementsOf( commands.roleSetup );
        assertThat( newCommands.userSetup ).containsExactlyInAnyOrderElementsOf( commands.userSetup );
    }

    @Test
    void shouldRestoreWithDbNameChange()
    {
        // GIVEN
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE DATABASE foo" );
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT REMOVE LABEL * ON GRAPH foo TO role" );
            tx.execute( "CREATE USER user SET PASSWORD 'password'" );
            tx.execute( "GRANT ROLE role TO user" );
            tx.commit();
        }

        BackupCommands commands = getBackupCommands( system, "foo", true, true );

        GraphDatabaseAPI newSystem = getCleanSystemDB();

        try ( Transaction tx = system.beginTx() )
        {
            // WHEN
            commands.roleSetup.forEach( c -> tx.execute( c, Map.of( "database", "bar" ) ) );
            commands.userSetup.forEach( tx::execute );
            tx.commit();
        }
        BackupCommands newCommands = getBackupCommands( newSystem, "bar", true, true );

        // THEN
        assertThat( newCommands.roleSetup ).containsExactlyInAnyOrderElementsOf( commands.roleSetup );
        assertThat( newCommands.userSetup ).containsExactlyInAnyOrderElementsOf( commands.userSetup );
    }

    @Test
    void shouldNotSaveDbmsPrivileges()
    {
        // GIVEN
        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT EXECUTE PROCEDURE * ON DBMS TO role" );
            tx.execute( "GRANT CREATE ROLE ON DBMS TO role" );
            tx.execute( "GRANT ALL ON DBMS TO role" );
            tx.commit();
        }

        // WHEN
        BackupCommands commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true );

        // THEN
        assertThat( commands.roleSetup ).containsExactlyInAnyOrder(
                String.format( "CREATE DATABASE $%s IF NOT EXISTS", DB_PARAM )
        );
        assertThat( commands.userSetup ).isEmpty();
    }

    /**
     * Get a clean system database, only keeping Version node, system database and default database nodes, and PUBLIC role.
     */
    private GraphDatabaseAPI getCleanSystemDB()
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
                if ( !(node.hasLabel( Label.label( "Database" ) ) && node.getProperty( "name" ).equals( DEFAULT_DATABASE_NAME )) &&
                     !(node.hasLabel( Label.label( "Database" ) ) && node.getProperty( "name" ).equals( SYSTEM_DATABASE_NAME )) &&
                     !node.hasLabel( Label.label( "Version" ) ) &&
                     !(node.hasLabel( Label.label( "Role" ) ) && node.getProperty( "name" ).equals( PUBLIC )) )
                {
                    node.delete();
                }
            }
            tx.commit();
        }
        return (GraphDatabaseAPI) system;
    }

    private List<String> getGrantFor( String role, ResourcePrivilege.GrantOrDeny privilegeType, PrivilegeAction action, Resource resource, Segment segment )
            throws InvalidArgumentsException
    {
        ResourcePrivilege privilege = new ResourcePrivilege( privilegeType, action, resource, segment, ResourcePrivilege.SpecialDatabase.ALL );
        return privilege.isDbmsPrivilege() ? Collections.emptyList() : privilege.asGrantFor( role, DB_PARAM );
    }

    private BackupCommands getBackupCommands( GraphDatabaseAPI system, String databaseName, boolean saveUsers, boolean saveRoles )
    {
        var component = system.getDependencyResolver().resolveDependency( EnterpriseSecurityGraphComponent.class );
        try ( Transaction tx = system.beginTx() )
        {
            return component.getBackupCommands( tx, databaseName, saveUsers, saveRoles );
        }
    }

    private void assertRecreatesOriginal( BackupCommands backup, String databaseName, boolean saveRoles )
    {
        GraphDatabaseAPI system = getCleanSystemDB();
        try ( Transaction tx = system.beginTx() )
        {
            backup.roleSetup.forEach( c -> tx.execute( c, Map.of( DB_PARAM, databaseName ) ) );
            backup.userSetup.forEach( tx::execute );
            tx.commit();
        }
        BackupCommands newBackup = getBackupCommands( system, databaseName, true, true );
        assertThat( newBackup.roleSetup ).containsExactlyInAnyOrderElementsOf( backup.roleSetup );

        if ( saveRoles )
        {
            assertThat( newBackup.userSetup ).containsExactlyInAnyOrderElementsOf( backup.userSetup );
        }
        else
        {
            assertThat( newBackup.userSetup ).isEmpty();
        }
    }
}
