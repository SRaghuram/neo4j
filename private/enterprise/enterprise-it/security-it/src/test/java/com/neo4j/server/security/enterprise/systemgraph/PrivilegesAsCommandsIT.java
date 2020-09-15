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
        // GIVEN
        GraphDatabaseService system = dbms.database( SYSTEM_DATABASE_NAME );

        // WHEN
        var privileges = getPrivilegesAsCommands( system, DEFAULT_DATABASE_NAME, true );

        // THEN
        assertThat( privileges ).isNotEmpty();
        assertRecreatesOriginal( privileges, DEFAULT_DATABASE_NAME, true );
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
        var privileges = getPrivilegesAsCommands( system, "graph.db", true );

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
        var privileges = getPrivilegesAsCommands( system, DEFAULT_DATABASE_NAME, true );

        // THEN
        assertThat( privileges ).containsExactly(
                "CREATE ROLE `role` IF NOT EXISTS",
                "GRANT ACCESS ON DATABASE neo4j TO `role`"
        );
        assertRecreatesOriginal( privileges, DEFAULT_DATABASE_NAME, true );
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
        var privileges = getPrivilegesAsCommands( system, DEFAULT_DATABASE_NAME, true );

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

    private List<String> getPrivilegesAsCommands( GraphDatabaseService system, String databaseName, boolean saveUsers )
    {
        var component = gdb.getDependencyResolver().resolveDependency( EnterpriseSecurityGraphComponent.class );
        try ( Transaction tx = system.beginTx() )
        {
            return component.getPrivilegesAsCommands( tx, databaseName, saveUsers );
        }
    }

    private void assertRecreatesOriginal( List<String> privileges, String databaseName, boolean saveUsers )
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
        assertThat( getPrivilegesAsCommands( system, databaseName, saveUsers ) ).containsExactlyInAnyOrderElementsOf( privileges );
    }
}
