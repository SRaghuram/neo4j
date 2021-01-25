/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.causalclustering.catchup.v4.metadata.DatabaseSecurityCommands;
import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.FunctionSegment;
import org.neo4j.internal.kernel.api.security.LabelSegment;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.ProcedureSegment;
import org.neo4j.internal.kernel.api.security.RelTypeSegment;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.internal.kernel.api.security.UserSegment;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.security.auth.SecurityTestUtils;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ACCESS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ALTER_USER;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ASSIGN_PRIVILEGE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ASSIGN_ROLE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CONSTRAINT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_CONSTRAINT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_ELEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_INDEX;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_LABEL;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_PROPERTYKEY;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_RELTYPE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_ROLE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_USER;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DATABASE_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DATABASE_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DBMS_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DELETE_ELEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DROP_CONSTRAINT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DROP_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DROP_INDEX;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DROP_ROLE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DROP_USER;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE_ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE_BOOSTED;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.GRAPH_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.INDEX;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.MATCH;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.MERGE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.PRIVILEGE_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.READ;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.REMOVE_LABEL;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.REMOVE_PRIVILEGE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.REMOVE_ROLE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ROLE_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SET_LABEL;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SET_PASSWORDS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SET_PROPERTY;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SET_USER_DEFAULT_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SET_USER_STATUS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_CONNECTION;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_CONSTRAINT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_INDEX;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_PRIVILEGE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_ROLE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_TRANSACTION;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_USER;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.START_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.STOP_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TERMINATE_CONNECTION;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TERMINATE_TRANSACTION;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TOKEN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TRANSACTION_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TRAVERSE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.USER_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.WRITE;

class PrivilegesAsCommandsIT
{
    static final String DB_PARAM = "database";
    static Config defaultConfig = Config.defaults( GraphDatabaseSettings.auth_enabled, true );
    static DatabaseManagementService dbms = new TestEnterpriseDatabaseManagementServiceBuilder().setConfig( defaultConfig ).impermanent().build();
    static GraphDatabaseAPI system = (GraphDatabaseAPI) dbms.database( SYSTEM_DATABASE_NAME );
    static EnterpriseAuthManager authManager = system.getDependencyResolver().resolveDependency( EnterpriseAuthManager.class );

    @AfterAll
    static void teardown() throws Exception
    {
        dbms.shutdown();
        authManager.shutdown();
    }

    @BeforeEach
    void setupTest()
    {
        cleanSystemDB();
    }

    @Test
    void shouldConstructValidPrivileges() throws InvalidArgumentsException
    {
        List<String> commands = new ArrayList<>();
        String role = "role";
        String user = "user";

        Predicate<PrivilegeAction> usesDatabaseResourceAndGeneralSegment = action ->
                ACCESS == action ||
                CREATE_LABEL == action ||
                CREATE_RELTYPE == action ||
                CREATE_PROPERTYKEY == action ||
                CREATE_INDEX == action ||
                DROP_INDEX == action ||
                SHOW_INDEX == action ||
                CREATE_CONSTRAINT == action ||
                DROP_CONSTRAINT == action ||
                SHOW_CONSTRAINT == action ||
                START_DATABASE == action ||
                STOP_DATABASE == action ||
                CREATE_DATABASE == action ||
                DROP_DATABASE == action ||
                SHOW_USER == action ||
                CREATE_USER == action ||
                SET_USER_STATUS == action ||
                SET_PASSWORDS == action ||
                SET_USER_DEFAULT_DATABASE == action ||
                DROP_USER == action ||
                SHOW_ROLE == action ||
                CREATE_ROLE == action ||
                DROP_ROLE == action ||
                ASSIGN_ROLE == action ||
                REMOVE_ROLE == action ||
                SHOW_PRIVILEGE == action ||
                ASSIGN_PRIVILEGE == action ||
                REMOVE_PRIVILEGE == action ||

                // collections of multiple actions
                ADMIN == action ||
                TOKEN == action ||
                CONSTRAINT == action ||
                INDEX == action ||
                DATABASE_MANAGEMENT == action ||
                USER_MANAGEMENT == action ||
                ALTER_USER == action ||
                ROLE_MANAGEMENT == action ||
                PRIVILEGE_MANAGEMENT == action ||
                DATABASE_ACTIONS == action ||
                DBMS_ACTIONS == action;

        Predicate<PrivilegeAction> usesGraphResource = action ->
                TRAVERSE == action ||
                WRITE == action ||
                CREATE_ELEMENT == action ||
                DELETE_ELEMENT == action ||

                // collection of multiple actions
                GRAPH_ACTIONS == action;

        Predicate<PrivilegeAction> usesPropertyResource = action ->
                READ == action ||
                SET_PROPERTY == action ||

                // collections of multiple actions
                MERGE == action ||
                MATCH == action;

        Predicate<PrivilegeAction> usesLabelResource = action ->
                SET_LABEL == action ||
                REMOVE_LABEL == action;

        for ( ResourcePrivilege.GrantOrDeny privilegeType : ResourcePrivilege.GrantOrDeny.values() )
        {
            for ( PrivilegeAction action : PrivilegeAction.values() )
            {
                if ( SHOW_TRANSACTION == action ||
                     TERMINATE_TRANSACTION == action ||
                     SHOW_CONNECTION == action ||
                     TERMINATE_CONNECTION == action ||
                     TRANSACTION_MANAGEMENT == action )
                {
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.DatabaseResource(), UserSegment.ALL ) );
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.DatabaseResource(), new UserSegment( "A" ) ) );
                }
                else if ( EXECUTE == action ||
                          EXECUTE_BOOSTED == action ||
                          EXECUTE_ADMIN == action )
                {
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.DatabaseResource(), ProcedureSegment.ALL ) );
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.DatabaseResource(), new ProcedureSegment( "A" ) ) );
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.DatabaseResource(), FunctionSegment.ALL ) );
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.DatabaseResource(), new FunctionSegment( "A" ) ) );
                }
                else if ( usesDatabaseResourceAndGeneralSegment.test( action ) )
                {
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.DatabaseResource(), Segment.ALL ) );
                }
                else if ( usesGraphResource.test( action ) )
                {
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.GraphResource(), LabelSegment.ALL ) );
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.GraphResource(), new LabelSegment( "A" ) ) );
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.GraphResource(), RelTypeSegment.ALL ) );
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.GraphResource(), new RelTypeSegment( "A" ) ) );
                }
                else if ( usesPropertyResource.test( action ) )
                {
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.AllPropertiesResource(), LabelSegment.ALL ) );
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.AllPropertiesResource(), new LabelSegment( "A" ) ) );
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.AllPropertiesResource(), RelTypeSegment.ALL ) );
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.AllPropertiesResource(), new RelTypeSegment( "A" ) ) );
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.PropertyResource( "A" ), LabelSegment.ALL ) );
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.PropertyResource( "A" ), new LabelSegment( "A" ) ) );
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.PropertyResource( "A" ), RelTypeSegment.ALL ) );
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.PropertyResource( "A" ), new RelTypeSegment( "A" ) ) );
                }
                else if ( usesLabelResource.test( action ) )
                {
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.AllLabelsResource(), LabelSegment.ALL ) );
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.AllLabelsResource(), new LabelSegment( "A" ) ) );
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.LabelResource( "A" ), LabelSegment.ALL ) );
                    commands.addAll( getCommandsFor( role, privilegeType, action, new Resource.LabelResource( "A" ), new LabelSegment( "A" ) ) );
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
            tx.commit();
        }

        for ( int i = 0; i < commands.size(); i++ )
        {
            if ( i % 2 == 0 )
            { // grant or deny command
                try ( Transaction tx = system.beginTx() )
                {
                    // all grant or deny commands should be valid Cypher syntax
                    tx.execute( commands.get( i ), Map.of( DB_PARAM, DEFAULT_DATABASE_NAME ) );
                    tx.commit();

                    DatabaseSecurityCommands databaseSecurityCommands = getBackupCommands( system, DEFAULT_DATABASE_NAME, false, true );
                    assertThat( databaseSecurityCommands.roleSetup ).filteredOn( c -> !c.startsWith( "CREATE" ) )
                                                                    .containsExactlyElementsOf( List.of( commands.get( i ) ) );
                }
            }
            else
            { // revoke command
                try ( Transaction tx = system.beginTx() )
                {
                    // all revoke commands should be valid Cypher syntax
                    tx.execute( commands.get( i ), Map.of( DB_PARAM, DEFAULT_DATABASE_NAME ) );
                    tx.commit();
                }
            }
        }
        DatabaseSecurityCommands databaseSecurityCommands = getBackupCommands( system, DEFAULT_DATABASE_NAME, false, true );
        assertThat( databaseSecurityCommands.roleSetup ).filteredOn( c -> !c.startsWith( "CREATE" ) )
                                                        .containsExactlyElementsOf( Collections.emptyList() );
    }

    @Test
    void shouldReplicateInitialState()
    {
        // GIVEN
        DatabaseManagementService dbms = new TestEnterpriseDatabaseManagementServiceBuilder().impermanent().build();
        GraphDatabaseAPI system = (GraphDatabaseAPI) dbms.database( SYSTEM_DATABASE_NAME );

        // WHEN
        DatabaseSecurityCommands commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true );

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
        DatabaseSecurityCommands commands = getBackupCommands( system, "graph.db", true, true );

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
        DatabaseSecurityCommands commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true );

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
        DatabaseSecurityCommands commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true );

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
        DatabaseSecurityCommands commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true );

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
            tx.execute( "CREATE USER user SET PASSWORD 'abc123' CHANGE NOT REQUIRED" );
            tx.execute( "GRANT ROLE role TO user" );
            tx.commit();
        }

        // WHEN
        DatabaseSecurityCommands commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true );

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
        assertCanLogin( "user", "abc123", AuthenticationResult.SUCCESS );
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
        DatabaseSecurityCommands commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true );

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
        DatabaseSecurityCommands commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true );

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
        assertCanLogin( "user", "abc123", AuthenticationResult.PASSWORD_CHANGE_REQUIRED );
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
        DatabaseSecurityCommands commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, false, true );

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
            tx.execute( "CREATE USER foo SET PASSWORD 'secret'" );
            tx.execute( "CREATE USER bar SET PASSWORD 'm0r3s3Cr37' CHANGE NOT REQUIRED" );

            tx.execute( "GRANT ROLE role TO foo" );
            tx.execute( "GRANT ROLE role TO bar" );
            tx.commit();
        }

        // WHEN
        DatabaseSecurityCommands commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, false );

        // THEN
        assertThat( commands.roleSetup ).containsExactlyInAnyOrder(
                String.format( "CREATE DATABASE $%s IF NOT EXISTS", DB_PARAM )
        );
        assertThat( commands.userSetup ).filteredOn( c -> c.startsWith( "CREATE USER `foo` IF NOT EXISTS SET ENCRYPTED PASSWORD" ) ).hasSize( 1 );
        assertThat( commands.userSetup ).filteredOn( c -> c.startsWith( "CREATE USER `bar` IF NOT EXISTS SET ENCRYPTED PASSWORD" ) ).hasSize( 1 );
        assertThat( commands.userSetup ).hasSize( 2 );
        assertRecreatesOriginal( commands, DEFAULT_DATABASE_NAME, false );
        assertCanLogin( "foo", "secret", AuthenticationResult.PASSWORD_CHANGE_REQUIRED );
        assertCanLogin( "bar", "m0r3s3Cr37", AuthenticationResult.SUCCESS );
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
        DatabaseSecurityCommands commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, false, false );

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
        DatabaseSecurityCommands commands = getBackupCommands( system, "newDb", true, true );

        // THEN
        assertThat( commands.roleSetup ).containsExactlyInAnyOrder(
                String.format( "CREATE DATABASE $%s IF NOT EXISTS", DB_PARAM ),
                "CREATE ROLE `role` IF NOT EXISTS",
                String.format( "GRANT ACCESS ON DATABASE $%s TO `role`", DB_PARAM )
        );
        assertRecreatesOriginal( commands, "newDb", true );
    }

    @Test
    void shouldIgnoreDbCase()
    {
        // GIVEN
        String database = "ThisIsWeirDCaSE";

        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE DATABASE ThisIsWeirDCaSE" );
            tx.execute( "CREATE ROLE role" );
            tx.execute( "GRANT ACCESS ON DATABASE * TO role" );
            tx.execute( "CREATE USER foo SET PASSWORD 'abc123'" );
            tx.execute( "GRANT ROLE role TO foo" );
            tx.commit();
        }

        // WHEN
        DatabaseSecurityCommands weirdCaseCommands = getBackupCommands( system, database.toLowerCase(), true, true );
        DatabaseSecurityCommands neo4jCommands = getBackupCommands( system, "NEo4J", true, true );

        // THEN
        assertThat( weirdCaseCommands.roleSetup )
                .containsExactlyInAnyOrderElementsOf( neo4jCommands.roleSetup )
                .containsExactlyInAnyOrder(
                    String.format( "CREATE DATABASE $%s IF NOT EXISTS", DB_PARAM ),
                    "CREATE ROLE `role` IF NOT EXISTS",
                    String.format( "GRANT ACCESS ON DATABASE $%s TO `role`", DB_PARAM )
                );
        assertRecreatesOriginal( weirdCaseCommands, "newDb", true );
        assertRecreatesOriginal( neo4jCommands, "newDb", true );
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

        DatabaseSecurityCommands commands = getBackupCommands( system, "newDb", true, true );
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

        DatabaseSecurityCommands commands = getBackupCommands( system, "database", true, true );

        cleanSystemDB();

        try ( Transaction tx = system.beginTx() )
        {
            tx.execute( "CREATE DATABASE database" );

            // WHEN
            commands.roleSetup.forEach( c -> tx.execute( c, Map.of( "database", "database" ) ) );
            commands.userSetup.forEach( tx::execute );
            tx.commit();
        }
        var newCommands = getBackupCommands( system, "database", true, true );

        // THEN
        assertThat( newCommands.roleSetup ).containsExactlyInAnyOrderElementsOf( commands.roleSetup );
        assertThat( newCommands.userSetup ).containsExactlyInAnyOrderElementsOf( commands.userSetup );
    }

    @Test
    void shouldRestoreWithAllConflicting()
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

        DatabaseSecurityCommands commands = getBackupCommands( system, "database", true, true );

        try ( Transaction tx = system.beginTx() )
        {
            // WHEN
            commands.roleSetup.forEach( c -> tx.execute( c, Map.of( "database", "database" ) ) );
            commands.userSetup.forEach( tx::execute );
            tx.commit();
        }

        DatabaseSecurityCommands newCommands = getBackupCommands( system, "database", true, true );

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

        DatabaseSecurityCommands commands = getBackupCommands( system, "foo", true, true );

        cleanSystemDB();

        try ( Transaction tx = system.beginTx() )
        {
            // WHEN
            commands.roleSetup.forEach( c -> tx.execute( c, Map.of( "database", "bar" ) ) );
            commands.userSetup.forEach( tx::execute );
            tx.commit();
        }
        DatabaseSecurityCommands newCommands = getBackupCommands( system, "bar", true, true );

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
        DatabaseSecurityCommands commands = getBackupCommands( system, DEFAULT_DATABASE_NAME, true, true );

        // THEN
        assertThat( commands.roleSetup ).containsExactlyInAnyOrder(
                String.format( "CREATE DATABASE $%s IF NOT EXISTS", DB_PARAM )
        );
        assertThat( commands.userSetup ).isEmpty();
    }

    /**
     * Get a clean system database, only keeping Version node, system database and default database nodes, and PUBLIC role.
     */
    private void cleanSystemDB()
    {
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
    }

    private List<String> getCommandsFor( String role, ResourcePrivilege.GrantOrDeny privilegeType, PrivilegeAction action, Resource resource,
                                         Segment segment ) throws InvalidArgumentsException
    {
        ResourcePrivilege privilege = new ResourcePrivilege( privilegeType, action, resource, segment, ResourcePrivilege.SpecialDatabase.ALL );
        List<String> commands = privilege.isDbmsPrivilege() ? Collections.emptyList() : privilege.asCommandFor( false, role, DB_PARAM );
        List<String> revokeCommands = privilege.isDbmsPrivilege() ? Collections.emptyList() : privilege.asCommandFor( true, role, DB_PARAM );

        List<String> res = new ArrayList<String>( commands );
        res.addAll( revokeCommands );
        return res;
    }

    private DatabaseSecurityCommands getBackupCommands( GraphDatabaseAPI system, String databaseName, boolean saveUsers, boolean saveRoles )
    {
        var component = system.getDependencyResolver().resolveDependency( EnterpriseSecurityGraphComponent.class );
        try ( Transaction tx = system.beginTx() )
        {
            return component.getBackupCommands( tx, databaseName, saveUsers, saveRoles );
        }
    }

    private void assertRecreatesOriginal( DatabaseSecurityCommands backup, String databaseName, boolean saveRoles )
    {
        cleanSystemDB();
        try ( Transaction tx = system.beginTx() )
        {
            backup.roleSetup.forEach( c -> tx.execute( c, Map.of( DB_PARAM, databaseName ) ) );
            backup.userSetup.forEach( tx::execute );
            tx.commit();
        }
        DatabaseSecurityCommands newBackup = getBackupCommands( system, databaseName, true, true );
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

    private void assertCanLogin( String username, String password, AuthenticationResult expectedResult )
    {
        try
        {
            EnterpriseLoginContext login = authManager.login( SecurityTestUtils.authToken( username, password ) );
            AuthenticationResult result = login.subject().getAuthenticationResult();
            assertThat( result ).isEqualTo( expectedResult );
        }
        catch ( InvalidAuthTokenException e )
        {
            fail();
        }
    }
}
