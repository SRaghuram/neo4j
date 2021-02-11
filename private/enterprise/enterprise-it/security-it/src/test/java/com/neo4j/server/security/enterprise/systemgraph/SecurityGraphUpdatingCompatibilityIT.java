/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.EnumSource.Mode.EXCLUDE;

class SecurityGraphUpdatingCompatibilityIT extends SecurityGraphCompatibilityTestBase
{
    static final String[] GRANT_REVOKE = {"GRANT", "REVOKE"};
    static final String[] GRANT_DENY_REVOKE = {"GRANT", "DENY", "REVOKE"};

    static Map<EnterpriseSecurityGraphComponentVersion,Set<PrivilegeCommand>> PRIVILEGES_PER_VERSION = Map.of(
            EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_40, Set.of(
                    // role management
                    new PrivilegeCommand( "CREATE ROLE ON DBMS", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "DROP ROLE ON DBMS", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "ASSIGN ROLE ON DBMS", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "REMOVE ROLE ON DBMS", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "SHOW ROLE ON DBMS", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "ROLE MANAGEMENT ON DBMS", GRANT_DENY_REVOKE ),

                    // database actions
                    new PrivilegeCommand( "ACCESS ON DATABASE *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "START ON DATABASE *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "STOP ON DATABASE *", GRANT_DENY_REVOKE ),

                    // index + constraints actions
                    new PrivilegeCommand( "CREATE INDEX ON DATABASE *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "DROP INDEX ON DATABASE *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "INDEX MANAGEMENT ON DATABASE *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "CREATE CONSTRAINT ON DATABASE *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "DROP CONSTRAINT ON DATABASE *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "CONSTRAINT MANAGEMENT ON DATABASE *", GRANT_DENY_REVOKE ),

                    // name management
                    new PrivilegeCommand( "CREATE NEW LABEL ON DATABASE *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "CREATE NEW TYPE ON DATABASE *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "CREATE NEW PROPERTY NAME ON DATABASE *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "NAME MANAGEMENT ON DATABASE *", GRANT_DENY_REVOKE ),

                    new PrivilegeCommand( "ALL ON DATABASE *", GRANT_DENY_REVOKE ),

                    // graph privileges
                    new PrivilegeCommand( "TRAVERSE ON GRAPH *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "READ {*} ON GRAPH *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "MATCH {*} ON GRAPH *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "WRITE ON GRAPH *", GRANT_DENY_REVOKE )
            ),
            EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_41D1, Set.of(
                    // user management
                    new PrivilegeCommand( "CREATE USER ON DBMS", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "DROP USER ON DBMS", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "SHOW USER ON DBMS", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "ALTER USER ON DBMS", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "USER MANAGEMENT ON DBMS", GRANT_DENY_REVOKE ),

                    // databases management
                    new PrivilegeCommand( "CREATE DATABASE ON DBMS", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "DROP DATABASE ON DBMS", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "DATABASE MANAGEMENT ON DBMS", GRANT_DENY_REVOKE ),

                    // privilege management
                    new PrivilegeCommand( "SHOW PRIVILEGE ON DBMS", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "ASSIGN PRIVILEGE ON DBMS", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "REMOVE PRIVILEGE ON DBMS", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "PRIVILEGE MANAGEMENT ON DBMS", GRANT_DENY_REVOKE ),

                    new PrivilegeCommand( "ALL ON DBMS", GRANT_DENY_REVOKE ),

                    //transaction management
                    new PrivilegeCommand( "TRANSACTION ON DATABASE *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "SHOW TRANSACTIONS ON DATABASE *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "TERMINATE TRANSACTIONS ON DATABASE *", GRANT_DENY_REVOKE ),

                    // default database
                    new PrivilegeCommand( "ACCESS ON DEFAULT DATABASE", GRANT_DENY_REVOKE )
            ),
            EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_41, Set.of(
                    // user management
                    new PrivilegeCommand( "SET USER STATUS ON DBMS", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "SET PASSWORDS ON DBMS", GRANT_DENY_REVOKE ),

                    // fine-grained writes
                    new PrivilegeCommand( "CREATE ON GRAPH * NODES *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "CREATE ON GRAPH * RELATIONSHIPS *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "CREATE ON GRAPH *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "DELETE ON GRAPH * NODES *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "DELETE ON GRAPH * RELATIONSHIPS *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "DELETE ON GRAPH *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "SET LABEL * ON GRAPH *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "REMOVE LABEL * ON GRAPH *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "SET PROPERTY {*} ON GRAPH *", GRANT_DENY_REVOKE ),

                    new PrivilegeCommand( "ALL PRIVILEGES ON GRAPH *", GRANT_DENY_REVOKE ),

                    new PrivilegeCommand( "MERGE {*} ON GRAPH *", GRANT_REVOKE )
            ),
            EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_42D4, Set.of(
                    new PrivilegeCommand( "EXECUTE PROCEDURE * ON DBMS", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "EXECUTE BOOSTED PROCEDURE * ON DBMS", GRANT_DENY_REVOKE )
            ),
            EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_42D6, Set.of(
                    new PrivilegeCommand( "EXECUTE FUNCTION * ON DBMS", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "EXECUTE BOOSTED FUNCTION * ON DBMS", GRANT_DENY_REVOKE )
            ),
            EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_42D7, Set.of(
                    new PrivilegeCommand( "SHOW INDEX ON DATABASE *", GRANT_DENY_REVOKE ),
                    new PrivilegeCommand( "SHOW CONSTRAINT ON DATABASE *", GRANT_DENY_REVOKE )
            )
    );

    @ParameterizedTest
    @EnumSource( value = EnterpriseSecurityGraphComponentVersion.class, mode = EXCLUDE, names = {"ENTERPRISE_SECURITY_UNKNOWN_VERSION",
                                                                                                 "ENTERPRISE_SECURITY_FUTURE_VERSION",
                                                                                                 "ENTERPRISE_SECURITY_FAKE_VERSION",
                                                                                                 "ENTERPRISE_SECURITY_35",
                                                                                                 "ENTERPRISE_SECURITY_36"} )
    void shouldAllowCompatibleUpdatingCommands( EnterpriseSecurityGraphComponentVersion version ) throws Exception
    {
        initEnterprise( version );
        for ( Map.Entry<EnterpriseSecurityGraphComponentVersion,Set<PrivilegeCommand>> entry : PRIVILEGES_PER_VERSION.entrySet() )
        {
            if ( entry.getKey().getVersion() <= version.getVersion() )
            {
                try ( Transaction tx = system.beginTransaction( KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED ) )
                {
                    for ( PrivilegeCommand privilegeCommand : entry.getValue() )
                    {
                        for ( String command : privilegeCommand.asCypher() )
                        {
                            tx.execute( command );
                        }
                    }
                    tx.commit();
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource( value = EnterpriseSecurityGraphComponentVersion.class, mode = EXCLUDE, names = {"ENTERPRISE_SECURITY_UNKNOWN_VERSION",
                                                                                                 "ENTERPRISE_SECURITY_FUTURE_VERSION",
                                                                                                 "ENTERPRISE_SECURITY_FAKE_VERSION",
                                                                                                 "ENTERPRISE_SECURITY_35",
                                                                                                 "ENTERPRISE_SECURITY_36"} )
    void shouldFailOnNewCommandsOnOldGraph( EnterpriseSecurityGraphComponentVersion version ) throws Exception
    {
        initEnterprise( version );
        for ( Map.Entry<EnterpriseSecurityGraphComponentVersion,Set<PrivilegeCommand>> entry : PRIVILEGES_PER_VERSION.entrySet() )
        {
            if ( entry.getKey().getVersion() > version.getVersion() )
            {
                for ( PrivilegeCommand privilegeCommand : entry.getValue() )
                {
                    for ( String command : privilegeCommand.asCypher() )
                    {
                        try ( Transaction tx = system.beginTransaction( KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED ) )
                        {
                            var exception = assertThrows( UnsupportedOperationException.class, () -> tx.execute( command ), command );
                            assertThat( exception.getMessage() )
                                    .contains( "This operation is not supported while running in compatibility mode with version " +
                                               version.getDescription() );
                        }
                    }
                }
            }
        }
    }

    static class PrivilegeCommand
    {
        private final String command;
        private final String[] privTypes;

        PrivilegeCommand( String command, String[] privTypes )
        {
            this.command = command;
            this.privTypes = privTypes;
        }

        HashSet<String> asCypher()
        {
            HashSet<String> queries = new HashSet<>();
            for ( String privType : privTypes )
            {
                switch ( privType )
                {
                case "GRANT":
                case "DENY":
                    queries.add( String.format( "%s %s TO reader", privType, command ) );
                    break;
                case "REVOKE":
                    queries.add( String.format( "%s %s FROM reader", privType, command ) );
                    break;
                default:
                    throw new RuntimeException( String.format( "Failure in setup of test, %s is not a valid privilege command", privType ) );
                }
            }
            return queries;
        }

        @Override
        public String toString()
        {
            return command;
        }
    }
}
