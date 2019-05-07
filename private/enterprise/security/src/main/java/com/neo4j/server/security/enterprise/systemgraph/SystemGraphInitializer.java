/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.CommercialSecurityModule;
import com.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.Action;
import com.neo4j.server.security.enterprise.auth.RoleRecord;
import com.neo4j.server.security.enterprise.auth.RoleRepository;
import org.apache.shiro.authz.SimpleRole;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.commandline.admin.security.SetDefaultAdminCommand;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.ListSnapshot;
import org.neo4j.server.security.auth.SecureHasher;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.server.security.systemgraph.BasicSystemGraphInitializer;
import org.neo4j.server.security.systemgraph.QueryExecutor;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.api.security.UserManager.INITIAL_USER_NAME;

public class SystemGraphInitializer extends BasicSystemGraphInitializer
{
    private final SystemGraphOperations systemGraphOperations;
    private final SystemGraphImportOptions importOptions;
    private final Log log;

    public SystemGraphInitializer( QueryExecutor queryExecutor, SystemGraphOperations systemGraphOperations,
            SystemGraphImportOptions importOptions, SecureHasher secureHasher, Log log )
    {
        super( queryExecutor, systemGraphOperations, importOptions.migrationUserRepositorySupplier,
                importOptions.initialUserRepositorySupplier, secureHasher, log );

        this.systemGraphOperations = systemGraphOperations;
        this.importOptions = importOptions;
        this.log = log;
    }

    @Override
    public void initializeSystemGraph() throws Exception
    {
        // If the system graph has not been initialized (typically the first time you start neo4j with the system graph auth provider)
        // we set it up with auth data in the following order:
        // 1) Do we have import files from running the `neo4j-admin import-auth` command?
        // 2) Otherwise, are there existing users and roles in the internal flat file realm, and are we allowed to migrate them to the system graph?
        // 3) If no users or roles were imported or migrated, create the predefined roles and one default admin user
        if ( isSystemGraphEmpty() )
        {
            // Ensure that multiple users, roles or databases cannot have the same name and are indexed
            final QueryResult.QueryResultVisitor<RuntimeException> resultVisitor = row -> true;
            queryExecutor.executeQuery( "CREATE CONSTRAINT ON (u:User) ASSERT u.name IS UNIQUE", Collections.emptyMap(), resultVisitor );
            queryExecutor.executeQuery( "CREATE CONSTRAINT ON (r:Role) ASSERT r.name IS UNIQUE", Collections.emptyMap(), resultVisitor );
            queryExecutor.executeQuery( "CREATE CONSTRAINT ON (d:Database) ASSERT d.name IS UNIQUE", Collections.emptyMap(), resultVisitor );

            ensureDefaultDatabases();

            if ( importOptions.shouldPerformImport )
            {
                importUsersAndRoles();
            }
            else if ( importOptions.mayPerformMigration )
            {
                migrateFromFlatFileRealm();
            }
        }
        else if ( importOptions.shouldPerformImport )
        {
            importUsersAndRoles();
        }

        // If no users or roles were imported we setup the
        // default predefined roles and user and make sure we have an admin user
        ensureDefaultUserAndRoles();
    }

    private boolean isSystemGraphEmpty()
    {
        // Execute a query to see if the system database exists
        String query = "MATCH (db:Database {name: $name}) RETURN db.name";
        Map<String,Object> params = map( "name", SYSTEM_DATABASE_NAME );

        return !queryExecutor.executeQueryWithParamCheck( query, params );
    }

    private void ensureDefaultUserAndRoles() throws Exception
    {
        if ( noUsers() )
        {
            ensureDefaultUser();
            ensureDefaultRoles( INITIAL_USER_NAME );
        }
        else if ( noRoles() )
        {
            // This will be the case when upgrading from community to enterprise system-graph
            String newAdmin = ensureAdmin();
            ensureDefaultRoles( newAdmin );
        }
    }

    private void ensureDefaultDatabases() throws InvalidArgumentsException
    {
        newDb( DEFAULT_DATABASE_NAME );
        newDb( SYSTEM_DATABASE_NAME );
    }

    /* Tries to find an admin candidate among the existing users */
    private String ensureAdmin( ) throws Exception
    {
        String newAdmin = null;

        // Try to import the name of a single admin user as set by the SetDefaultAdmin command
        if ( importOptions.defaultAdminRepositorySupplier != null )
        {
            UserRepository defaultAdminRepository = startUserRepository( importOptions.defaultAdminRepositorySupplier );
            final int numberOfDefaultAdmins = defaultAdminRepository.numberOfUsers();
            if ( numberOfDefaultAdmins > 1 )
            {
                throw new InvalidArgumentsException( "No roles defined, and multiple users defined as default admin user." + " Please use `neo4j-admin " +
                        SetDefaultAdminCommand.COMMAND_NAME + "` to select a valid admin." );
            }
            else if ( numberOfDefaultAdmins == 1 )
            {
                newAdmin = defaultAdminRepository.getAllUsernames().iterator().next();
            }

            stopUserRepository( defaultAdminRepository );
        }

        Set<String> usernames = systemGraphOperations.getAllUsernames();

        if ( newAdmin != null )
        {
            // We currently support only one default admin
            if ( systemGraphOperations.getUser( newAdmin, true ) == null )
            {
                throw new InvalidArgumentsException( "No roles defined, and default admin user '" + newAdmin + "' does not exist. Please use `neo4j-admin " +
                        SetDefaultAdminCommand.COMMAND_NAME + "` to select a valid admin." );
            }
            return newAdmin;
        }
        else if ( usernames.size() == 1 )
        {
            // If only a single user exists, make her an admin
            return usernames.iterator().next();
        }
        else if ( usernames.contains( INITIAL_USER_NAME ) )
        {
            // If the default neo4j user exists, make her an admin
            return INITIAL_USER_NAME;
        }
        else
        {
            throw new InvalidArgumentsException(
                    "No roles defined, and cannot determine which user should be admin. " + "Please use `neo4j-admin " + SetDefaultAdminCommand.COMMAND_NAME +
                            "` to select an " + "admin." );
        }
    }

    /* Builds all predefined roles if no roles exist. Adds newAdmin to admin role */
    private void ensureDefaultRoles( String newAdmin ) throws Exception
    {
        if ( noRoles() )
        {
            // Create the predefined roles
            for ( String role : PredefinedRolesBuilder.roles.keySet() )
            {
                systemGraphOperations.newRole( role );
                assignDefaultPrivileges( role );
            }
        }

        // Actually assign the admin role
        systemGraphOperations.addRoleToUser( PredefinedRoles.ADMIN, newAdmin );
        log.info( "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, newAdmin );
    }

    private void assignDefaultPrivileges( String roleName ) throws InvalidArgumentsException
    {
        if ( PredefinedRolesBuilder.roles.containsKey( roleName ) )
        {
            SimpleRole simpleRole = PredefinedRolesBuilder.roles.get( roleName );
            if ( simpleRole.isPermitted( PredefinedRolesBuilder.SYSTEM ) )
            {
                systemGraphOperations.grantPrivilegeToRole( roleName, new ResourcePrivilege( Action.WRITE, new Resource.SystemResource() ) );
            }
            if ( simpleRole.isPermitted( PredefinedRolesBuilder.SCHEMA ) )
            {
                systemGraphOperations.grantPrivilegeToRole( roleName, new ResourcePrivilege( Action.WRITE, new Resource.SchemaResource() ) );
            }
            if ( simpleRole.isPermitted( PredefinedRolesBuilder.TOKEN ) )
            {
                systemGraphOperations.grantPrivilegeToRole( roleName, new ResourcePrivilege( Action.WRITE, new Resource.TokenResource() ) );
            }
            if ( simpleRole.isPermitted( PredefinedRolesBuilder.WRITE ) )
            {
                systemGraphOperations.grantPrivilegeToRole( roleName, new ResourcePrivilege( Action.WRITE, new Resource.GraphResource() ) );
            }
            if ( simpleRole.isPermitted( PredefinedRolesBuilder.READ ) )
            {
                systemGraphOperations.grantPrivilegeToRole( roleName, new ResourcePrivilege( Action.READ, new Resource.GraphResource() ) );
            }
        }
    }

    private void migrateFromFlatFileRealm() throws Exception
    {
        UserRepository userRepository = startUserRepository( importOptions.migrationUserRepositorySupplier );
        RoleRepository roleRepository = startRoleRepository( importOptions.migrationRoleRepositorySupplier );

        boolean importOk = doImportUsersAndRoles( userRepository, roleRepository, /* !purgeOnSuccess */ false );
        if ( !importOk )
        {
            throw new InvalidArgumentsException(
                    "Automatic migration of users and roles into system graph failed because repository files are inconsistent. " +
                            "Please use `neo4j-admin " + CommercialSecurityModule.IMPORT_AUTH_COMMAND_NAME + "` to perform migration manually." );
        }

        stopUserRepository( userRepository );
        stopRoleRepository( roleRepository );
    }

    private void importUsersAndRoles() throws Exception
    {
        UserRepository userRepository = startUserRepository( importOptions.importUserRepositorySupplier );
        RoleRepository roleRepository = startRoleRepository( importOptions.importRoleRepositorySupplier );

        boolean importOK = doImportUsersAndRoles( userRepository, roleRepository, importOptions.shouldPurgeImportRepositoriesAfterSuccesfulImport );
        if ( !importOK )
        {
            throw new InvalidArgumentsException(
                    "Import of users and roles into system graph failed because the import files are inconsistent. " +
                            "Please use `neo4j-admin " + CommercialSecurityModule.IMPORT_AUTH_COMMAND_NAME + "` to retry import again." );
        }

        stopUserRepository( userRepository );
        stopRoleRepository( roleRepository );
    }

    private void newDb( String dbName ) throws InvalidArgumentsException
    {
        SystemGraphOperations.assertValidDbName( dbName );

        String query = "CREATE (db:Database {name: $dbName, status: 'online'})";
        Map<String,Object> params = Collections.singletonMap( "dbName", dbName );

        queryExecutor.executeQueryWithConstraint( query, params, "The specified database '" + dbName + "' already exists." );
    }

    private boolean noRoles()
    {
        String query = "MATCH (r:Role) RETURN count(r)";
        return queryExecutor.executeQueryLong( query ) == 0;
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

    private boolean doImportUsersAndRoles( UserRepository userRepository, RoleRepository roleRepository, boolean purgeOnSuccess ) throws Exception
    {
        ListSnapshot<User> users = userRepository.getPersistedSnapshot();
        ListSnapshot<RoleRecord> roles = roleRepository.getPersistedSnapshot();

        boolean isEmpty = users.values().isEmpty() && roles.values().isEmpty();
        boolean valid = RoleRepository.validate( users.values(), roles.values() );

        if ( !valid )
        {
            return false;
        }

        if ( !isEmpty )
        {
            Pair<Integer,Integer> numberOfDeletedUsersAndRoles = Pair.of( 0, 0 );

            try ( Transaction transaction = queryExecutor.beginTx() )
            {
                // If a reset of all existing auth data was requested we do it within the same transaction as the import
                if ( importOptions.shouldResetSystemGraphAuthBeforeImport )
                {
                    numberOfDeletedUsersAndRoles = deleteAllSystemGraphAuthData();
                }

                // This is not an efficient implementation, since it executes many queries
                // If performance ever becomes an issue we could do this with a single query instead
                for ( User user : users.values() )
                {
                    systemGraphOperations.addUser( user );
                }
                for ( RoleRecord role : roles.values() )
                {
                    systemGraphOperations.newRole( role.name() );
                    assignDefaultPrivileges( role.name() );

                    for ( String username : role.users() )
                    {
                        systemGraphOperations.addRoleToUser( role.name(), username );
                    }
                }
                transaction.success();
            }

            assert validateImportSucceeded( userRepository, roleRepository );

            // Log what happened to the security log
            if ( importOptions.shouldResetSystemGraphAuthBeforeImport )
            {
                String userString = numberOfDeletedUsersAndRoles.first() == 1 ? "user" : "users";
                String roleString = numberOfDeletedUsersAndRoles.other() == 1 ? "role" : "roles";

                log.info( "Deleted %s %s and %s %s into system graph.",
                        Integer.toString( numberOfDeletedUsersAndRoles.first() ), userString,
                        Integer.toString( numberOfDeletedUsersAndRoles.other() ), roleString );
            }
            {
                String userString = users.values().size() == 1 ? "user" : "users";
                String roleString = roles.values().size() == 1 ? "role" : "roles";

                log.info( "Completed import of %s %s and %s %s into system graph.",
                        Integer.toString( users.values().size() ), userString,
                        Integer.toString( roles.values().size() ), roleString );
            }

        }

        // If transaction succeeded, we purge the repositories so that we will not try to import them again the next time we restart
        if ( purgeOnSuccess )
        {
            userRepository.purge();
            roleRepository.purge();

            log.debug( "Source import user and role repositories were purged." );
        }
        return true;
    }

    /**
     * This method should delete all existing auth data from the system graph.
     * It is used in preparation for an import where the admin has requested
     * a reset of the auth graph.
     */
    private Pair<Integer,Integer> deleteAllSystemGraphAuthData() throws InvalidArgumentsException
    {
        // This is not an efficient implementation, since it executes many queries
        // If performance becomes an issue we could do this with a single query instead

        Set<String> usernames = systemGraphOperations.getAllUsernames();
        for ( String username : usernames )
        {
            systemGraphOperations.deleteUser( username );
        }

        Set<String> roleNames = systemGraphOperations.getAllRoleNames();
        for ( String roleName : roleNames )
        {
            systemGraphOperations.deleteRole( roleName );
        }

        // TODO: Delete Database nodes? (Only if they are exclusively used by the security module)

        return Pair.of( usernames.size(), roleNames.size() );
    }

    private boolean validateImportSucceeded( UserRepository userRepository, RoleRepository roleRepository ) throws Exception
    {
        // Take a new snapshot of the import repositories
        ListSnapshot<User> users = userRepository.getPersistedSnapshot();
        ListSnapshot<RoleRecord> roles = roleRepository.getPersistedSnapshot();

        try ( Transaction transaction = queryExecutor.beginTx() )
        {
            Set<String> systemGraphUsers = systemGraphOperations.getAllUsernames();
            List<String> repoUsernames = users.values().stream().map( User::name ).collect( Collectors.toList() );
            if ( !systemGraphUsers.containsAll( repoUsernames ) )
            {
                throw new IOException( "Users were not imported correctly" );
            }

            List<String> repoRoleNames = roles.values().stream().map( RoleRecord::name ).collect( Collectors.toList() );
            Set<String> systemGraphRoles = systemGraphOperations.getAllRoleNames();
            if ( !systemGraphRoles.containsAll( repoRoleNames ) )
            {
                throw new IOException( "Roles were not imported correctly" );
            }

            for ( RoleRecord role : roles.values() )
            {
                Set<String> usernamesForRole = systemGraphOperations.getUsernamesForRole( role.name() );
                if ( !usernamesForRole.containsAll( role.users() ) )
                {
                    throw new IOException( "Role assignments were not imported correctly" );
                }
            }

            transaction.success();
        }
        return true;
    }
}
