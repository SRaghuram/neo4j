/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.InMemoryRoleRepository;
import com.neo4j.server.security.enterprise.auth.RoleRecord;
import com.neo4j.server.security.enterprise.auth.RoleRepository;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.systemgraph.versions.KnownEnterpriseSecurityComponentVersion;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DefaultSystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_single_automatic_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;
import static org.neo4j.dbms.database.ComponentVersion.Neo4jVersions.VERSION_35;
import static org.neo4j.dbms.database.ComponentVersion.Neo4jVersions.VERSION_36;
import static org.neo4j.dbms.database.ComponentVersion.Neo4jVersions.VERSION_40;
import static org.neo4j.dbms.database.ComponentVersion.Neo4jVersions.VERSION_41D1;

@EphemeralTestDirectoryExtension
abstract class SecurityGraphCompatibilityTestBase
{
    @SuppressWarnings( "unused" )
    @Inject
    private TestDirectory directory;

    protected DatabaseManagementService dbms;
    EnterpriseSecurityGraphComponent enterpriseComponent;
    private RoleRepository roleRepository;

    GraphDatabaseAPI system;
    AuthManager authManager;

    @BeforeEach
    void setup() throws Exception
    {
        Config cfg = Config.newBuilder()
                .set( auth_enabled, TRUE )
                .set( allow_single_automatic_upgrade, FALSE )
                .build();

        UserRepository userRepository = new InMemoryUserRepository();
        roleRepository = new InMemoryRoleRepository();
        Log securityLog = mock( Log.class );
        var communityComponent = new UserSecurityGraphComponent( securityLog, userRepository, userRepository, cfg );
        enterpriseComponent = new EnterpriseSecurityGraphComponent( securityLog, roleRepository, userRepository, cfg );
        var testSystemGraphComponents = new TestSystemGraphComponents( new DefaultSystemGraphComponent( cfg ), communityComponent );
        // We explicitly do not add enterpriseComponents to the component initializer, so we can initialize per test class

        Dependencies deps = new Dependencies();
        deps.satisfyDependencies( testSystemGraphComponents );

        TestEnterpriseDatabaseManagementServiceBuilder builder = new TestEnterpriseDatabaseManagementServiceBuilder( directory.homePath() )
                .impermanent()
                .setConfig( cfg )
                .setExternalDependencies( deps );
        dbms = builder.build();
        system = (GraphDatabaseAPI) dbms.database( SYSTEM_DATABASE_NAME );
        DependencyResolver platformDependencies = system.getDependencyResolver();
        authManager = platformDependencies.resolveDependency( AuthManager.class );
        initEnterprise();
    }

    void initEnterprise() throws Exception
    {
    }

    @AfterEach
    void teardown()
    {
        if ( dbms != null )
        {
            dbms.shutdown();
            dbms = null;
            system = null;
        }
    }

    void initEnterprise( String version ) throws Exception
    {
        List<String> roles;
        switch ( version )
        {
        case VERSION_35:
            // Version 3.5 does not have roles in the system graph, we must assign all the default roles to a user in order for them to be migrated
            roleRepository.create( new RoleRecord( ADMIN, INITIAL_USER_NAME ) );
            roleRepository.create( new RoleRecord( ARCHITECT, INITIAL_USER_NAME ) );
            roleRepository.create( new RoleRecord( PUBLISHER, INITIAL_USER_NAME ) );
            roleRepository.create( new RoleRecord( EDITOR, INITIAL_USER_NAME ) );
            roleRepository.create( new RoleRecord( READER, INITIAL_USER_NAME ) );
            return;
        case VERSION_36:
        case VERSION_40:
            // Versions older than 41 drop 01 should not have PUBLIC role
            roles = List.of( ADMIN, ARCHITECT, PUBLISHER, EDITOR, READER );
            break;
        default:
            roles = PredefinedRoles.roles;
        }
        initEnterprise( version, roles );
    }

    private void initEnterprise( String version, List<String> roles ) throws Exception
    {
        KnownEnterpriseSecurityComponentVersion builder = enterpriseComponent.findSecurityGraphComponentVersion( version );
        try ( Transaction tx = system.beginTx() )
        {
            enterpriseComponent.initializeSystemGraphConstraints( tx );
            tx.commit();
        }
        try ( Transaction tx = system.beginTx() )
        {
            builder.initializePrivileges( tx, roles, Map.of( ADMIN, Set.of( INITIAL_USER_NAME ) ) );
            switch ( version )
            {
            case VERSION_36:
            case VERSION_40:
            case VERSION_41D1:
                // Versions older than 41 drop 01 should not have a version set
                break;
            default:
                // have to manually set the version property
                builder.setVersionProperty( tx, builder.version );
            }
            tx.commit();
        }
    }

    public static class TestSystemGraphComponents extends SystemGraphComponents
    {
        TestSystemGraphComponents( SystemGraphComponent... components )
        {
            for ( SystemGraphComponent component : components )
            {
                super.register( component );
            }
        }

        @Override
        public void register( SystemGraphComponent component )
        {
            // Do nothing in tests
        }

        void override( SystemGraphComponent component )
        {
            super.register( component );
        }
    }

    private static final String[] GRANT_REVOKE = {"GRANT", "REVOKE"};
    private static final String[] GRANT_DENY_REVOKE = {"GRANT", "DENY", "REVOKE"};

    static Set<PrivilegeCommand> PRIVILEGES_ADDED_IN_40 = Set.of(
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
    );

    static Set<PrivilegeCommand> PRIVILEGES_ADDED_IN_41D1 = Set.of(
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

    );

    static Set<PrivilegeCommand> PRIVILEGES_ADDED_IN_41 = Set.of(
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
    );

    static Set<PrivilegeCommand> PRIVILEGES_ADDED_IN_42D4 = Set.of(
            new PrivilegeCommand( "EXECUTE PROCEDURE * ON DBMS", GRANT_DENY_REVOKE ),
            new PrivilegeCommand( "EXECUTE BOOSTED PROCEDURE * ON DBMS", GRANT_DENY_REVOKE )
    );

    static Set<PrivilegeCommand> PRIVILEGES_ADDED_IN_42D6 = Set.of(
            new PrivilegeCommand( "EXECUTE FUNCTION * ON DBMS", GRANT_DENY_REVOKE ),
            new PrivilegeCommand( "EXECUTE BOOSTED FUNCTION * ON DBMS", GRANT_DENY_REVOKE )
    );

    static Set<PrivilegeCommand> PRIVILEGES_ADDED_IN_42D7 = Set.of(
            new PrivilegeCommand( "SHOW INDEX ON DATABASE *", GRANT_DENY_REVOKE ),
            new PrivilegeCommand( "SHOW CONSTRAINT ON DATABASE *", GRANT_DENY_REVOKE )
    );

    static Set<PrivilegeCommand> PRIVILEGES_ADDED_IN_43D2 = Set.of(
            new PrivilegeCommand( "SET USER DEFAULT DATABASE ON DBMS", GRANT_DENY_REVOKE )
    );

    static Set<PrivilegeCommand> ALL_PRIVILEGES = new HashSet<>();

    static
    {
        ALL_PRIVILEGES.addAll( PRIVILEGES_ADDED_IN_40 );
        ALL_PRIVILEGES.addAll( PRIVILEGES_ADDED_IN_41D1 );
        ALL_PRIVILEGES.addAll( PRIVILEGES_ADDED_IN_41 );
        ALL_PRIVILEGES.addAll( PRIVILEGES_ADDED_IN_42D4 );
        ALL_PRIVILEGES.addAll( PRIVILEGES_ADDED_IN_42D6 );
        ALL_PRIVILEGES.addAll( PRIVILEGES_ADDED_IN_42D7 );
        ALL_PRIVILEGES.addAll( PRIVILEGES_ADDED_IN_43D2 );
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
