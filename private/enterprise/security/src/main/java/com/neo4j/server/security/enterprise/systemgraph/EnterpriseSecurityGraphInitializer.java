/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.RoleRecord;
import com.neo4j.server.security.enterprise.auth.RoleRepository;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.apache.shiro.authz.SimpleRole;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.SystemGraphInitializer;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.ListSnapshot;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.systemgraph.UserSecurityGraphInitializer;

import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;
import static org.neo4j.server.security.systemgraph.BasicSystemGraphRealm.assertValidUsername;

public class EnterpriseSecurityGraphInitializer extends UserSecurityGraphInitializer
{
    private final SystemGraphImportOptions importOptions;
    private Label ROLE_LABEL = Label.label( "Role" );
    private Label PRIVILEGE_LABEL = Label.label( "Privilege" );

    private List<Node> roleNodes = new ArrayList<>();

    private RelationshipType GRANTED = RelationshipType.withName( "GRANTED" );
    private RelationshipType USER_TO_ROLE = RelationshipType.withName( "HAS_ROLE" );
    private RelationshipType SCOPE = RelationshipType.withName( "SCOPE" );
    private RelationshipType APPLIES_TO = RelationshipType.withName( "APPLIES_TO" );
    private RelationshipType QUALIFIED = RelationshipType.withName( "QUALIFIED" );
    private RelationshipType FOR = RelationshipType.withName( "FOR" );

    private Node traverseNodePriv;
    private Node traverserRelPriv;
    private Node readNodePriv;
    private Node readRelPriv;
    private Node writeNodePriv;
    private Node writeRelPriv;
    private Node accessPriv;
    private Node tokenPriv;
    private Node schemaPriv;
    private Node adminPriv;

    public EnterpriseSecurityGraphInitializer( DatabaseManager<?> databaseManager, SystemGraphInitializer systemGraphInitializer, Log log,
            SystemGraphImportOptions importOptions, SecureHasher secureHasher )
    {
        super( databaseManager, systemGraphInitializer, log, importOptions.migrationUserRepositorySupplier, importOptions.initialUserRepositorySupplier,
                secureHasher );

        this.importOptions = importOptions;
    }

    @Override
    public void initializeSecurityGraph() throws Exception
    {
        initializeSecurityGraph( getSystemDb() );
    }

    @Override
    public void initializeSecurityGraph( GraphDatabaseService database ) throws Exception
    {
        systemGraphInitializer.initializeSystemGraph( database );
        systemDb = database;
        doInitializeSecurityGraph();
    }

    private void doInitializeSecurityGraph() throws Exception
    {
        // Must be done outside main transaction since it changes the schema
        setupConstraints();

        try ( Transaction tx = systemDb.beginTx() )
        {
            userNodes = findInitialNodes( tx, USER_LABEL );
            userNodes.forEach( node -> usernames.add( (String) node.getProperty( "name" ) ) );
            roleNodes = findInitialNodes( tx, ROLE_LABEL );

            // Perform migration if all of the following are true:
            // 1) The system graph has not been initialized (typically the first time you start neo4j).
            // 2) There exists users and/or roles in the internal flat file realm
            if ( userNodes.isEmpty() )
            {
                migrateFromFlatFileRealm( tx );
            }

            // If no users or roles were migrated we setup the
            // default predefined roles and user and make sure we have an admin user
            ensureDefaultUserAndRoles( tx );
            tx.commit();
        }
    }

    private void setupConstraints()
    {
        // Ensure that multiple roles cannot have the same name and are indexed
        try ( Transaction tx = systemDb.beginTx() )
        {
            try
            {
                tx.schema().constraintFor( USER_LABEL ).assertPropertyIsUnique( "name" ).create();
                tx.schema().constraintFor( ROLE_LABEL ).assertPropertyIsUnique( "name" ).create();
            }
            catch ( ConstraintViolationException e )
            {
                // Makes the creation of constraints for security idempotent
                if ( !e.getMessage().startsWith( "An equivalent constraint already exists" ) )
                {
                    throw e;
                }
            }
            tx.commit();
        }
    }

    private void ensureDefaultUserAndRoles( Transaction tx ) throws Exception
    {
        if ( userNodes.isEmpty() )
        {
            addDefaultUser( tx );
            ensureDefaultRolesAndPrivileges( tx, INITIAL_USER_NAME );
        }
        else if ( roleNodes.isEmpty() )
        {
            // This will be the case when upgrading from community to enterprise system-graph
            try
            {
                String newAdmin = ensureAdmin();
                ensureDefaultRolesAndPrivileges( tx, newAdmin );
            }
            catch ( InvalidArgumentsException e )
            {
                // Should still add users even if failed to decide who should be admin
                tx.commit();
                throw e;
            }
        }

        // If applicable, give the default user the password set by set-initial-password command
        setInitialPassword();
    }

    /* Tries to find an admin candidate among the existing users */
    private String ensureAdmin() throws Exception
    {
        String newAdmin = null;

        // Try to import the name of a single admin user as set by the SetDefaultAdmin command
        if ( importOptions.defaultAdminRepositorySupplier != null )
        {
            UserRepository defaultAdminRepository = startUserRepository( importOptions.defaultAdminRepositorySupplier );
            final int numberOfDefaultAdmins = defaultAdminRepository.numberOfUsers();
            if ( numberOfDefaultAdmins > 1 )
            {
                throw new InvalidArgumentsException( "No roles defined, and multiple users defined as default admin user. " + "Please use " +
                        "`neo4j-admin set-default-admin` to select a valid admin." );
            }
            else if ( numberOfDefaultAdmins == 1 )
            {
                newAdmin = defaultAdminRepository.getAllUsernames().iterator().next();
            }

            stopUserRepository( defaultAdminRepository );
        }

        if ( newAdmin != null )
        {
            // We currently support only one default admin
            if ( !usernames.contains( newAdmin ) )
            {
                throw new InvalidArgumentsException( "No roles defined, and default admin user '" + newAdmin + "' does not exist. " + "Please use " +
                        "`neo4j-admin set-default-admin` to select a valid admin." );
            }
            return newAdmin;
        }
        else if ( usernames.size() == 1 )
        {
            // If only a single user exists, make her an admin
            return usernames.get( 0 );
        }
        else if ( usernames.contains( INITIAL_USER_NAME ) )
        {
            // If the default neo4j user exists, make her an admin
            return INITIAL_USER_NAME;
        }
        else
        {
            throw new InvalidArgumentsException(
                    "No roles defined, and cannot determine which user should be admin. " + "Please use `neo4j-admin set-default-admin` to select an admin. " );
        }
    }

    /* Builds all predefined roles if no roles exist. Adds newAdmin to admin role */
    private void ensureDefaultRolesAndPrivileges( Transaction tx, String newAdmin ) throws Exception
    {
        if ( roleNodes.isEmpty() )
        {
            setUpDefaultPrivileges( tx );

            // Create the predefined roles
            PredefinedRolesBuilder.roles.forEach( ( roleName, simpleRole ) ->
            {
                Node role = newRole( tx, roleName );
                assignDefaultPrivileges( role, simpleRole );
            } );
        }

        // Actually assign the admin role
        Node admin = tx.findNode( ROLE_LABEL, "name", PredefinedRoles.ADMIN );

        addRoleToUser( tx, admin, newAdmin );
        log.info( "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, newAdmin );
    }

    private void setUpDefaultPrivileges( Transaction tx )
    {
        // Check for DatabaseAll node to see if the default privileges were already setup
        final ResourceIterator<Node> itr = tx.findNodes( Label.label( "DatabaseAll" ) );
        boolean foundNode = itr.hasNext();
        itr.close();

        if ( foundNode )
        {
            return;
        }

        // Create a DatabaseAll node
        Node allDb = tx.createNode( Label.label( "DatabaseAll" ) );
        allDb.setProperty( "name", "*" );

        // Create initial qualifier nodes
        Node labelQualifier = tx.createNode( Label.label( "LabelQualifierAll" ) );
        labelQualifier.setProperty( "type", "node" );
        labelQualifier.setProperty( "label", "*" );

        Node relQualifier = tx.createNode( Label.label( "RelationshipQualifierAll" ) );
        relQualifier.setProperty( "type", "relationship" );
        relQualifier.setProperty( "label", "*" );

        Node dbQualifier = tx.createNode( Label.label( "DatabaseQualifier" ) );
        dbQualifier.setProperty( "type", "database" );
        dbQualifier.setProperty( "label", "" );

        // Create initial segments nodes and connect them with DatabaseAll and qualifiers
        Label segmentLabel = Label.label( "Segment" );

        Node labelSegement = tx.createNode( segmentLabel );
        labelSegement.createRelationshipTo( labelQualifier, QUALIFIED );
        labelSegement.createRelationshipTo( allDb, FOR );

        Node relSegement = tx.createNode( segmentLabel );
        relSegement.createRelationshipTo( relQualifier, QUALIFIED );
        relSegement.createRelationshipTo( allDb, FOR );

        Node dbSegement = tx.createNode( segmentLabel );
        dbSegement.createRelationshipTo( dbQualifier, QUALIFIED );
        dbSegement.createRelationshipTo( allDb, FOR );

        // Create initial resource nodes
        Label resourceLabel = Label.label( "Resource" );

        Node graphResource = tx.createNode( resourceLabel );
        graphResource.setProperty( "type", Resource.Type.GRAPH.toString() );
        graphResource.setProperty( "arg1", "" );
        graphResource.setProperty( "arg2", "" );

        Node allPropResource = tx.createNode( resourceLabel );
        allPropResource.setProperty( "type", Resource.Type.ALL_PROPERTIES.toString() );
        allPropResource.setProperty( "arg1", "" );
        allPropResource.setProperty( "arg2", "" );

        Node dbResource = tx.createNode( resourceLabel );
        dbResource.setProperty( "type", Resource.Type.DATABASE.toString() );
        dbResource.setProperty( "arg1", "" );
        dbResource.setProperty( "arg2", "" );

        // Create initial privilege nodes and connect them with resources and segments
        traverseNodePriv = tx.createNode( PRIVILEGE_LABEL );
        traverserRelPriv = tx.createNode( PRIVILEGE_LABEL );
        readNodePriv = tx.createNode( PRIVILEGE_LABEL );
        readRelPriv = tx.createNode( PRIVILEGE_LABEL );
        writeNodePriv = tx.createNode( PRIVILEGE_LABEL );
        writeRelPriv = tx.createNode( PRIVILEGE_LABEL );
        accessPriv = tx.createNode( PRIVILEGE_LABEL );
        tokenPriv = tx.createNode( PRIVILEGE_LABEL );
        schemaPriv = tx.createNode( PRIVILEGE_LABEL );
        adminPriv = tx.createNode( PRIVILEGE_LABEL );

        setupPrivilegeNode( traverseNodePriv, PrivilegeAction.TRAVERSE, labelSegement, graphResource );
        setupPrivilegeNode( traverserRelPriv, PrivilegeAction.TRAVERSE, relSegement, graphResource );
        setupPrivilegeNode( readNodePriv, PrivilegeAction.READ, labelSegement, allPropResource );
        setupPrivilegeNode( readRelPriv, PrivilegeAction.READ, relSegement, allPropResource );
        setupPrivilegeNode( writeNodePriv, PrivilegeAction.WRITE, labelSegement, allPropResource );
        setupPrivilegeNode( writeRelPriv, PrivilegeAction.WRITE, relSegement, allPropResource );
        setupPrivilegeNode( accessPriv, PrivilegeAction.ACCESS, dbSegement, dbResource );
        setupPrivilegeNode( tokenPriv, PrivilegeAction.TOKEN, dbSegement, dbResource );
        setupPrivilegeNode( schemaPriv, PrivilegeAction.SCHEMA, dbSegement, dbResource );
        setupPrivilegeNode( adminPriv, PrivilegeAction.ADMIN, dbSegement, dbResource );
    }

    private void setupPrivilegeNode( Node privNode, PrivilegeAction action, Node segmentNode, Node resourceNode )
    {
        privNode.setProperty( "action", action.toString() );
        privNode.createRelationshipTo( segmentNode, SCOPE );
        privNode.createRelationshipTo( resourceNode, APPLIES_TO );
    }

    private void assignDefaultPrivileges( Node role, SimpleRole simpleRole )
    {
        if ( simpleRole.isPermitted( PredefinedRolesBuilder.SYSTEM ) )
        {
            role.createRelationshipTo( adminPriv, GRANTED );
        }
        if ( simpleRole.isPermitted( PredefinedRolesBuilder.SCHEMA ) )
        {
            role.createRelationshipTo( schemaPriv, GRANTED );
        }
        if ( simpleRole.isPermitted( PredefinedRolesBuilder.TOKEN ) )
        {
            role.createRelationshipTo( tokenPriv, GRANTED );
        }
        if ( simpleRole.isPermitted( PredefinedRolesBuilder.WRITE ) )
        {
            // The segment part is ignored for this action
            role.createRelationshipTo( writeNodePriv, GRANTED );
            role.createRelationshipTo( writeRelPriv, GRANTED );
        }
        if ( simpleRole.isPermitted( PredefinedRolesBuilder.READ ) )
        {
            role.createRelationshipTo( traverseNodePriv, GRANTED );
            role.createRelationshipTo( traverserRelPriv, GRANTED );
            role.createRelationshipTo( readNodePriv, GRANTED );
            role.createRelationshipTo( readRelPriv, GRANTED );
        }
        if ( simpleRole.isPermitted( PredefinedRolesBuilder.ACCESS ) )
        {
            role.createRelationshipTo( accessPriv, GRANTED );
        }
    }

    private void migrateFromFlatFileRealm( Transaction tx ) throws Exception
    {
        UserRepository userRepository = startUserRepository( importOptions.migrationUserRepositorySupplier );
        RoleRepository roleRepository = startRoleRepository( importOptions.migrationRoleRepositorySupplier );
        doImportUsers( tx, userRepository );
        boolean importOk = doImportRoles( tx, userRepository, roleRepository );
        if ( !importOk )
        {
            throw new InvalidArgumentsException(
                    "Automatic migration of users and roles into system graph failed because repository files are inconsistent. " );
        }

        stopUserRepository( userRepository );
        stopRoleRepository( roleRepository );
    }

    private RoleRepository startRoleRepository( Supplier<RoleRepository> supplier ) throws Exception
    {
        RoleRepository roleRepository = supplier.get();
        roleRepository.init();
        roleRepository.start();
        return roleRepository;
    }

    private void stopRoleRepository( RoleRepository roleRepository ) throws Exception
    {
        roleRepository.stop();
        roleRepository.shutdown();
    }

    private boolean doImportRoles( Transaction tx, UserRepository userRepository, RoleRepository roleRepository ) throws Exception
    {
        ListSnapshot<User> users = userRepository.getPersistedSnapshot();
        ListSnapshot<RoleRecord> roles = roleRepository.getPersistedSnapshot();

        boolean usersToImport = !users.values().isEmpty();
        boolean rolesToImport = !roles.values().isEmpty();
        boolean valid = RoleRepository.validate( users.values(), roles.values() );

        if ( !valid )
        {
            return false;
        }

        if ( rolesToImport )
        {
            setUpDefaultPrivileges( tx );
            for ( RoleRecord roleRecord : roles.values() )
            {
                String roleName = roleRecord.name();
                Node role = newRole( tx, roleName );

                if ( PredefinedRolesBuilder.roles.containsKey( roleName ) )
                {
                    SimpleRole simpleRole = PredefinedRolesBuilder.roles.get( roleName );
                    assignDefaultPrivileges( role, simpleRole );
                }

                for ( String username : roleRecord.users() )
                {
                    addRoleToUser( tx, role, username );
                }
            }
            assert validateImportSucceeded( tx, userRepository, roleRepository );
        }

        if ( usersToImport || rolesToImport )
        {
            // Log what happened to the security log
            String roleString = roles.values().size() == 1 ? "role" : "roles";
            log.info( "Completed import of %s %s into system graph.", Integer.toString( roles.values().size() ), roleString );
        }
        return true;
    }

    private boolean validateImportSucceeded( Transaction tx, UserRepository userRepository, RoleRepository roleRepository ) throws Exception
    {
        // Take a new snapshot of the import repositories
        ListSnapshot<User> users = userRepository.getPersistedSnapshot();
        ListSnapshot<RoleRecord> roles = roleRepository.getPersistedSnapshot();

        Set<String> systemGraphUsers = getAllNames( tx, USER_LABEL );
        List<String> repoUsernames = users.values().stream().map( User::name ).collect( Collectors.toList() );
        if ( !systemGraphUsers.containsAll( repoUsernames ) )
        {
            throw new IOException( "Users were not imported correctly" );
        }

        List<String> repoRoleNames = roles.values().stream().map( RoleRecord::name ).collect( Collectors.toList() );
        Set<String> systemGraphRoles = getAllNames( tx, ROLE_LABEL );
        if ( !systemGraphRoles.containsAll( repoRoleNames ) )
        {
            throw new IOException( "Roles were not imported correctly" );
        }

        for ( RoleRecord role : roles.values() )
        {
            Set<String> usernamesForRole = getUsernamesForRole( tx, role.name() );
            if ( !usernamesForRole.containsAll( role.users() ) )
            {
                throw new IOException( "Role assignments were not imported correctly" );
            }
        }

        return true;
    }

    private Node newRole( Transaction tx, String roleName )
    {
        Node node = tx.createNode( ROLE_LABEL );
        node.setProperty( "name", roleName );
        roleNodes.add( node );
        return node;
    }

    private void addRoleToUser( Transaction tx, Node role, String username ) throws InvalidArgumentsException
    {
        assertValidUsername( username );

        Node user = tx.findNode( USER_LABEL, "name", username );

        if ( user == null )
        {
            throw new InvalidArgumentsException( String.format( "User %s did not exist", username ) );
        }

        user.createRelationshipTo( role, USER_TO_ROLE );
    }

    private Set<String> getUsernamesForRole( Transaction tx, String roleName ) throws InvalidArgumentsException
    {
        Set<String> usernames = new HashSet<>();
        Node role = tx.findNode( ROLE_LABEL, "name", roleName );

        if ( role == null )
        {
            throw new InvalidArgumentsException( "Role did not eixst" );
        }

        final Iterable<Relationship> relationships = role.getRelationships( Direction.INCOMING );

        relationships.forEach( relationship -> usernames.add( (String) relationship.getStartNode().getProperty( "name" ) ) );

        return usernames;
    }

    private Set<String> getAllNames( Transaction tx, Label label )
    {
        ResourceIterator<Node> nodes = tx.findNodes( label );
        Set<String> usernames = nodes.stream().map( node -> (String) node.getProperty( "name" ) ).collect( Collectors.toSet() );
        nodes.close();
        return usernames;
    }
}
