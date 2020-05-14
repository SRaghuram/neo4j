/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SecurityGraphUpdatingCompatibility_40_IT extends SecurityGraphCompatibilityTestBase
{
    @Override
    void initEnterprise() throws Exception
    {
        initEnterprise( VERSION_40 );
    }

    @ParameterizedTest
    @ValueSource( strings = {
            // role management
            "CREATE ROLE ON DBMS",
            "DROP ROLE ON DBMS",
            "ASSIGN ROLE ON DBMS",
            "REMOVE ROLE ON DBMS",
            "SHOW ROLE ON DBMS",
            "ROLE MANAGEMENT ON DBMS",

            // database actions
            "ACCESS ON DATABASE *",
            "START ON DATABASE *",
            "STOP ON DATABASE *",

            // index + constraints actions
            "CREATE INDEX ON DATABASE *",
            "DROP INDEX ON DATABASE *",
            "INDEX MANAGEMENT ON DATABASE *",
            "CREATE CONSTRAINT ON DATABASE *",
            "DROP CONSTRAINT ON DATABASE *",
            "CONSTRAINT MANAGEMENT ON DATABASE *",

            // name management
            "CREATE NEW LABEL ON DATABASE *",
            "CREATE NEW TYPE ON DATABASE *",
            "CREATE NEW PROPERTY NAME ON DATABASE *",
            "NAME MANAGEMENT ON DATABASE *",
            "ALL ON DATABASE *"
    } )
    void shouldAllowCompatibleUpdatingCommandsOnOldGraph( String privilege )
    {
        try ( Transaction tx = system.beginTransaction( KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED ) )
        {
            tx.execute( String.format( "GRANT %s TO reader", privilege ) );
            tx.execute( String.format( "DENY %s TO reader", privilege ) );
            tx.execute( String.format( "REVOKE %s FROM reader", privilege ) );
            tx.commit();
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {
            // user management
            "CREATE USER ON DBMS",
            "DROP USER ON DBMS",
            "SHOW USER ON DBMS",
            "SET USER STATUS ON DBMS",
            "SET PASSWORDS ON DBMS",
            "ALTER USER ON DBMS",
            "USER MANAGEMENT ON DBMS",

            // databases management
            "CREATE DATABASE ON DBMS",
            "DROP DATABASE ON DBMS",
            "DATABASE MANAGEMENT ON DBMS",

            // privilege management
            "SHOW PRIVILEGE ON DBMS",
            "ASSIGN PRIVILEGE ON DBMS",
            "REMOVE PRIVILEGE ON DBMS",
            "PRIVILEGE MANAGEMENT ON DBMS",

            "ALL ON DBMS",

            //transaction management
            "TRANSACTION ON DATABASE *",
            "SHOW TRANSACTIONS ON DATABASE *",
            "TERMINATE TRANSACTIONS ON DATABASE *",

            // fine-grained writes
            "CREATE ON GRAPH * NODES *",
            "CREATE ON GRAPH * RELATIONSHIPS *",
            "CREATE ON GRAPH *",
            "DELETE ON GRAPH * NODES *",
            "DELETE ON GRAPH * RELATIONSHIPS *",
            "DELETE ON GRAPH *",
            "SET LABEL * ON GRAPH *",
            "REMOVE LABEL * ON GRAPH *",
            "SET PROPERTY {*} ON GRAPH *",

            "ALL PRIVILEGES ON GRAPH *",

            // default database
            "ACCESS ON DEFAULT DATABASE"
    } )
    void shouldFailOnNewCommandsOnOldGraph( String privilege )
    {
        for ( String template : List.of( "GRANT %s TO reader", "DENY %s TO reader", "REVOKE %s FROM reader" ) )
        {
            try ( Transaction tx = system.beginTransaction( KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED ) )
            {
                String query = String.format( template, privilege );
                var exception = assertThrows( UnsupportedOperationException.class, () -> tx.execute( query ), query );
                assertThat( exception.getMessage() )
                        .contains( "This operation is not supported while running in compatibility mode with version " + VERSION_40 );
            }
        }
    }
}
