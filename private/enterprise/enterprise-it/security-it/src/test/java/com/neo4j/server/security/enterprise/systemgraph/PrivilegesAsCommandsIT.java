/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@ImpermanentEnterpriseDbmsExtension
class PrivilegesAsCommandsIT
{
    @Inject
    DatabaseManagementService dbms;

    @Inject
    GraphDatabaseAPI gdb;

    @Test
    void shouldReplicateInitialState()
    {
        //TODO this test fails when run individually, but passes when entire class is run
        // GIVEN
        GraphDatabaseService system = dbms.database( SYSTEM_DATABASE_NAME );

        // WHEN
        var privileges = getPrivilegesAsCommands( system, DEFAULT_DATABASE_NAME, true, true);

        // THEN
        assertThat( privileges ).isNotEmpty();
        assertRecreatesOriginal( privileges, DEFAULT_DATABASE_NAME, true, true);
        assertIsOrdered( privileges );
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
        var privileges = getPrivilegesAsCommands( system, "graph.db", true, true);

        // THEN
        assertThat( privileges ).isEmpty();
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
        var privileges = getPrivilegesAsCommands( system, DEFAULT_DATABASE_NAME, true, true);

        // THEN
        assertThat( privileges ).containsExactly(
                "CREATE ROLE `role` IF NOT EXISTS",
                "GRANT ACCESS ON DATABASE neo4j TO `role`"
        );
        assertRecreatesOriginal( privileges, DEFAULT_DATABASE_NAME, true, true);
        assertIsOrdered( privileges );
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
        var privileges = getPrivilegesAsCommands( system, DEFAULT_DATABASE_NAME, true, true);

        // THEN
        assertThat( privileges ).hasSize( 3 );
        assertRecreatesOriginal( privileges, DEFAULT_DATABASE_NAME, true, true);
        assertIsOrdered( privileges );
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
        var privileges = getPrivilegesAsCommands( system, DEFAULT_DATABASE_NAME, true, true);

        // THEN
        assertThat( privileges ).isEmpty();
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
        var privileges = getPrivilegesAsCommands( system, DEFAULT_DATABASE_NAME, true, true);

        // THEN
        assertThat( privileges ).hasSize(4);
        assertRecreatesOriginal( privileges, DEFAULT_DATABASE_NAME, true, true);
        assertIsOrdered( privileges );
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

        var privileges = getPrivilegesAsCommands( system, DEFAULT_DATABASE_NAME, true, true);
        assertThat( privileges).filteredOn( p -> p.startsWith( "CREATE USER `user`" ) ).hasSize( 1 );
        assertRecreatesOriginal( privileges, DEFAULT_DATABASE_NAME, true, true);
        assertIsOrdered( privileges );
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

        var privileges = getPrivilegesAsCommands( system, DEFAULT_DATABASE_NAME, false, true );
        assertThat( privileges.stream().filter( p -> p.startsWith( "CREATE USER" ) ) ).isEmpty();
        assertRecreatesOriginal( privileges, DEFAULT_DATABASE_NAME, true, true);
        assertIsOrdered( privileges );
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
        var privileges = getPrivilegesAsCommands( system, DEFAULT_DATABASE_NAME, true, true);

        // THEN
        assertThat( privileges ).isEmpty();
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
                if ( !(node.hasLabel( Label.label( "Database" ) ) || node.hasLabel( Label.label( "Version" ) )) )
                {
                    for ( Relationship rel : node.getRelationships() )
                    {
                        rel.delete();
                    }
                    if ( !(node.hasLabel( Label.label( "Role" ) ) && node.getProperty( "name" ).equals( PUBLIC )) )
                    {
                        node.delete();
                    }
                }
            }
            tx.commit();
        }
        return system;
    }

    private List<String> getPrivilegesAsCommands( GraphDatabaseService system, String databaseName, boolean saveUsers, boolean saveRoles )
    {
        var component = gdb.getDependencyResolver().resolveDependency( EnterpriseSecurityGraphComponent.class );
        try ( Transaction tx = system.beginTx() )
        {
            return component.getPrivilegesAsCommands( tx, databaseName, saveUsers, saveRoles );
        }
    }

    private void assertRecreatesOriginal( List<String> privileges, String databaseName, boolean saveUsers, boolean saveRoles)
    {
        GraphDatabaseService system = getCleanSystemDB();
        try ( Transaction tx = system.beginTx() )
        {
            for ( String privilege : privileges )
            {
                tx.execute( privilege );
            }
            tx.commit();
        }
        assertThat( getPrivilegesAsCommands( system, databaseName, saveUsers, saveRoles) ).containsExactlyInAnyOrderElementsOf( privileges );
    }

    private void assertIsOrdered( List<String> privileges )
    {

        Pattern p = Pattern.compile(
                "^" +
                "(?:" + // ROLES
                    "CREATE ROLE `(?<role>.*)` .*" + // create a role 'x'
                    "(?:\\n(?:GRANT|DENY) .* `\\k<role>`)*" + // grant/deny any number of privileges to only role 'x'
                ")*" + // repeat for any number of roles
                "(?:" + // USERS
                    "\\nCREATE USER `(?<user>.*)` .*" + // create a user 'y'
                    "(?:\\nGRANT .*`\\k<user>`)*" + // grant any number of roles to only user 'y'
                ")*" + // repeat for any number of users
                "$"
        );

        Matcher m = p.matcher( String.join("\n" , privileges));

        assertThat(m.matches()).isTrue();
    }
}