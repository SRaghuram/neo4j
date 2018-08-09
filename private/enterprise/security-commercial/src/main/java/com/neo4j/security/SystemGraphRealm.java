/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import com.neo4j.dbms.database.MultiDatabaseManager;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.DisabledAccountException;
import org.apache.shiro.authc.ExcessiveAttemptsException;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.apache.shiro.authc.UnknownAccountException;
import org.apache.shiro.authc.credential.AllowAllCredentialsMatcher;
import org.apache.shiro.authc.pam.UnsupportedTokenException;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.neo4j.commandline.admin.security.SetDefaultAdminCommand;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.security.AuthProviderFailedException;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.PasswordPolicy;
import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.auth.UserSerialization;
import org.neo4j.server.security.auth.exception.FormatException;
import org.neo4j.server.security.enterprise.auth.EnterpriseUserManager;
import org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder;
import org.neo4j.server.security.enterprise.auth.RealmLifecycle;
import org.neo4j.server.security.enterprise.auth.SecureHasher;
import org.neo4j.server.security.enterprise.auth.ShiroAuthToken;
import org.neo4j.server.security.enterprise.auth.ShiroAuthenticationInfo;
import org.neo4j.server.security.enterprise.auth.ShiroAuthorizationInfoProvider;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * Shiro realm using a Neo4j graph to store users and roles
 */
class SystemGraphRealm extends AuthorizingRealm implements RealmLifecycle, EnterpriseUserManager, ShiroAuthorizationInfoProvider
{
    private final UserRepository initialUserRepository;
    private final UserRepository defaultAdminRepository;
    private final PasswordPolicy passwordPolicy;
    private final AuthenticationStrategy authenticationStrategy;
    private final boolean authenticationEnabled;
    private final boolean authorizationEnabled;
    private final SecureHasher secureHasher;
    private final SystemGraphExecutor systemGraphExecutor;

    /**
     * This flag is used in the same way as User.PASSWORD_CHANGE_REQUIRED, but it's
     * placed here because of user suspension not being a part of community edition
     */
    private static final String IS_SUSPENDED = "is_suspended";

    private final UserSerialization serialization = new UserSerialization();

   SystemGraphRealm( SystemGraphExecutor systemGraphExecutor, SecureHasher secureHasher, PasswordPolicy passwordPolicy,
           AuthenticationStrategy authenticationStrategy, boolean authenticationEnabled, boolean authorizationEnabled,
           UserRepository initialUserRepository, UserRepository defaultAdminRepository )
    {
        super();

        setName( SecuritySettings.SYSTEM_GRAPH_REALM_NAME );

        this.secureHasher = secureHasher;
        this.initialUserRepository = initialUserRepository;
        this.defaultAdminRepository = defaultAdminRepository;
        this.passwordPolicy = passwordPolicy;
        this.authenticationStrategy = authenticationStrategy;
        this.authenticationEnabled = authenticationEnabled;
        this.authorizationEnabled = authorizationEnabled;
        this.systemGraphExecutor = systemGraphExecutor;
        //setAuthenticationCachingEnabled( true );
        setAuthenticationCachingEnabled( false ); // TODO: Enable this after switching to use secureHasher for hashing credentials
        setAuthorizationCachingEnabled( true );
        //setCredentialsMatcher( secureHasher.getHashedCredentialsMatcher() );
        setCredentialsMatcher( new AllowAllCredentialsMatcher() ); // TODO: Switch to secureHasher.getHashedCredentialsMatcher()
        setRolePermissionResolver( PredefinedRolesBuilder.rolePermissionResolver );
    }

    @Override
    public void initialize() throws Throwable
    {
        // TODO: should we still have initialUserRepository and defaultAdminRepository?
        initialUserRepository.init();
        defaultAdminRepository.init();
    }

    @Override
    public void start() throws Throwable
    {
        initialUserRepository.start();
        defaultAdminRepository.start();

        // Ensure that multiple users, roles or databases cannot have the same name
        final QueryResult.QueryResultVisitor<RuntimeException> resultVisitor = row -> true;
        systemGraphExecutor.executeQuery( "CREATE CONSTRAINT ON (u:User) ASSERT u.name IS UNIQUE", Collections.emptyMap(), resultVisitor );
        systemGraphExecutor.executeQuery( "CREATE CONSTRAINT ON (r:Role) ASSERT r.name IS UNIQUE", Collections.emptyMap(), resultVisitor );
        systemGraphExecutor.executeQuery( "CREATE CONSTRAINT ON (d:Database) ASSERT d.name IS UNIQUE", Collections.emptyMap(), resultVisitor );

        // Setup default db, predefined roles and default users
        ensureDefaultDatabases();
        boolean wasDefaultRolesCreated = ensureDefaultRoles();
        ensureDefaultUsers( wasDefaultRolesCreated );
    }

    @Override
    public void stop() throws Throwable
    {
        initialUserRepository.stop();
        defaultAdminRepository.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        initialUserRepository.shutdown();
        defaultAdminRepository.shutdown();
    }

    @Override
    public boolean supports( AuthenticationToken token )
    {
        try
        {
            if ( token instanceof ShiroAuthToken )
            {
                ShiroAuthToken shiroAuthToken = (ShiroAuthToken) token;
                return shiroAuthToken.getScheme().equals( AuthToken.BASIC_SCHEME ) &&
                        (shiroAuthToken.supportsRealm( AuthToken.NATIVE_REALM ));
            }
            return false;
        }
        catch ( InvalidAuthTokenException e )
        {
            return false;
        }
    }

    @Override
    protected AuthenticationInfo doGetAuthenticationInfo( AuthenticationToken token ) throws AuthenticationException
    {
        if ( !authenticationEnabled )
        {
            return null;
        }

        ShiroAuthToken shiroAuthToken = (ShiroAuthToken) token;

        String username;
        String password;
        try
        {
            username = AuthToken.safeCast( AuthToken.PRINCIPAL, shiroAuthToken.getAuthTokenMap() );
            password = AuthToken.safeCast( AuthToken.CREDENTIALS, shiroAuthToken.getAuthTokenMap() );
        }
        catch ( InvalidAuthTokenException e )
        {
            throw new UnsupportedTokenException( e );
        }

        User user;
        try
        {
            user = getUser( username );
        }
        catch ( InvalidArgumentsException e )
        {
            throw new UnknownAccountException();
        }

        AuthenticationResult result = authenticationStrategy.authenticate( user, password );

        switch ( result )
        {
        case FAILURE:
            throw new IncorrectCredentialsException();
        case TOO_MANY_ATTEMPTS:
            throw new ExcessiveAttemptsException();
        default:
            break;
        }

        if ( user.hasFlag( IS_SUSPENDED ) )
        {
            throw new DisabledAccountException( "User '" + user.name() + "' is suspended." );
        }

        if ( user.passwordChangeRequired() )
        {
            result = AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
        }

        // NOTE: We do not cache the authentication info using the Shiro cache manager,
        // so all authentication request will go through this method.
        // Hence the credentials matcher is set to AllowAllCredentialsMatcher,
        // and we do not need to store hashed credentials in the AuthenticationInfo.
        // TODO: Do not forget to remove the comment above once we have switched to use SecureHasher for credentials and enabled authentication caching
        return new ShiroAuthenticationInfo( user.name(), getName() /* Realm name */, result );
    }

    @Override
    protected AuthorizationInfo doGetAuthorizationInfo( PrincipalCollection principals )
    {
        if ( !authorizationEnabled )
        {
            return null;
        }

        String username = (String) getAvailablePrincipal( principals );
        if ( username == null )
        {
            return null;
        }

        boolean[] existingUser = {false};
        boolean[] passwordChangeRequired = {false};
        boolean[] suspended = {false};
        Set<String> roleNames = new TreeSet<>();

        String query = "MATCH (u:User {name: $username}) OPTIONAL MATCH (u)-[:HAS_DB_ROLE]->(:DbRole)-[:FOR_ROLE]->(r:Role) " +
                "RETURN u.passwordChangeRequired, u.suspended, r.name";
        Map<String,Object> params = map( "username", username );

        final QueryResult.QueryResultVisitor<RuntimeException> resultVisitor = row ->
        {
            AnyValue[] fields = row.fields();
            existingUser[0] = true;
            passwordChangeRequired[0] = ((BooleanValue) fields[0]).booleanValue();
            suspended[0] = ((BooleanValue) fields[1]).booleanValue();

            Value role = (Value) fields[2];
            if ( role != Values.NO_VALUE )
            {
                roleNames.add( ((TextValue) role).stringValue() );
            }

            return true;
        };

        systemGraphExecutor.executeQuery( query, params, resultVisitor );

        if ( !existingUser[0] )
        {
            return null;
        }

        if ( passwordChangeRequired[0] || suspended[0] )
        {
            return new SimpleAuthorizationInfo();
        }

        return new SimpleAuthorizationInfo( roleNames );
    }

    @Override
    public AuthorizationInfo getAuthorizationInfoSnapshot( PrincipalCollection principalCollection )
    {
        return getAuthorizationInfo( principalCollection );
    }

    @Override
    public void suspendUser( String username ) throws InvalidArgumentsException
    {
        String query = "MATCH (u:User {name: $name}) SET u.suspended = true RETURN u.name";
        Map<String,Object> params = map( "name", username );
        String errorMsg = "User '" + username + "' does not exist.";

        systemGraphExecutor.executeQueryWithParamCheck( query, params, errorMsg );
        clearCacheForUser( username );
    }

    @Override
    public void activateUser( String username, boolean requirePasswordChange ) throws InvalidArgumentsException
    {
        String query = "MATCH (u:User {name: $name}) SET u.suspended = false, u.passwordChangeRequired = $passwordChangeRequired RETURN u.name";
        Map<String,Object> params = map( "name", username, "passwordChangeRequired", requirePasswordChange );
        String errorMsg = "User '" + username + "' does not exist.";

        systemGraphExecutor.executeQueryWithParamCheck( query, params, errorMsg );
        clearCacheForUser( username );
    }

    @Override
    public void newRole( String roleName, String... usernames ) throws InvalidArgumentsException
    {
        assertValidRoleName( roleName );

        String query = "CREATE (r:Role {name: $name})";
        Map<String,Object> params = Collections.singletonMap( "name", roleName );

        systemGraphExecutor.executeQueryWithConstraint( query, params,
                "The specified role '" + roleName + "' already exists." );

        // NOTE: This adding users thing is used by tests only so we do not need to optimize this into a more advanced single Cypher query
        for ( String username : usernames )
        {
            addRoleToUser( roleName, username );
        }
    }

    @Override
    public boolean deleteRole( String roleName ) throws InvalidArgumentsException
    {
        assertNotPredefinedRoleName( roleName );

        String query = "MATCH (r:Role {name: $name}) WITH r OPTIONAL MATCH (dbr:DbRole)-[:FOR_ROLE]->(r) " +
                "WITH r, r.name as name, dbr DETACH DELETE r, dbr RETURN name";

        Map<String,Object> params = map("name", roleName );
        String errorMsg = "Role '" + roleName + "' does not exist.";

        boolean success = systemGraphExecutor.executeQueryWithParamCheck( query, params, errorMsg );
        clearCachedAuthorizationInfo();
        return success;
    }

    @Override
    public void assertRoleExists( String roleName ) throws InvalidArgumentsException
    {
        String query = "MATCH (r:Role {name: $name}) RETURN r.name";
        Map<String,Object> params = map( "name", roleName );
        String errorMsg = "Role '" + roleName + "' does not exist.";

        systemGraphExecutor.executeQueryWithParamCheck( query, params, errorMsg );
    }

    @Override
    public void addRoleToUser( String roleName, String username ) throws InvalidArgumentsException
    {
        addRoleToUserForDb( roleName, DatabaseManager.DEFAULT_DATABASE_NAME, username );
    }

    private void addRoleToUserForDb( String roleName, String dbName, String username ) throws InvalidArgumentsException
    {
        assertValidRoleName( roleName );
        assertValidUsername( username );
        assertValidDbName( dbName );

        String query = "MATCH (u:User {name: $user}), (r:Role {name: $role}), (d:Database {name: $db})" +
                "MERGE (r)<-[:FOR_ROLE]-(dbr:DbRole)-[:FOR_DATABASE]->(d) MERGE (u)-[:HAS_DB_ROLE]->(dbr) RETURN dbr.id";
        Map<String,Object> params = map("user", username, "role", roleName, "db", dbName);

        boolean success = systemGraphExecutor.executeQueryWithParamCheck( query, params );

        if ( !success )
        {
            // We need to decide the cause of this failure
            getUser( username ); // This throws InvalidArgumentException if user does not exist
            assertRoleExists( roleName ); //This throws InvalidArgumentException if role does not exist
            assertDbExists( dbName ); //This throws InvalidArgument if db does not exist
            throw new AuthProviderFailedException( "Failed to execute query on auth graph" );
        }

        clearCachedAuthorizationInfoForUser( username );
    }

    @Override
    public void removeRoleFromUser( String roleName, String username ) throws InvalidArgumentsException
    {
        removeRoleFromUserForDb( roleName, DatabaseManager.DEFAULT_DATABASE_NAME, username );
    }

    private void removeRoleFromUserForDb( String roleName, String dbName, String username ) throws InvalidArgumentsException
    {
        assertValidRoleName( roleName );
        assertValidUsername( username );
        assertValidDbName( dbName );

        // TODO: Also remove the DbRoleNode if this was the last user connected to it?
        String query = "MATCH (u:User {name: $name})-[rel:HAS_DB_ROLE]->(dbr:DbRole) WITH dbr, rel " +
                "MATCH (r:Role {name: $role})<-[:FOR_ROLE]-(dbr)-[:FOR_DATABASE]->(db:Database {name: $db})" +
                "WITH dbr, rel DELETE rel RETURN dbr.id";

        Map<String,Object> params = map( "name", username, "role", roleName, "db", dbName);

        boolean success = systemGraphExecutor.executeQueryWithParamCheck( query, params );

        if ( !success )
        {
            // We need to decide the cause of this failure
            getUser( username ); // This throws InvalidArgumentException if user does not exist
            assertRoleExists( roleName ); // This throws InvalidArgumentException if role does not exist
            assertDbExists( dbName ); // This throws InvalidArgumentException if db does not exist
            // If the HAS_ROLE relationship does not exist we should silently fall through
        }

        clearCachedAuthorizationInfoForUser( username );
    }

    @Override
    public Set<String> getAllRoleNames()
    {
        String query = "MATCH (r:Role) RETURN r.name";
        return systemGraphExecutor.executeQueryWithResultSet( query );
    }

    @Override
    public Set<String> getRoleNamesForUser( String username ) throws InvalidArgumentsException
    {
        String query = "MATCH (u:User {name: $username}) OPTIONAL MATCH (u)-[:HAS_DB_ROLE]->(:DbRole)-[:FOR_ROLE]->(r:Role) RETURN r.name";
        Map<String,Object> params = map( "username", username );
        String errorMsg = "User '" + username + "' does not exist.";

        return systemGraphExecutor.executeQueryWithResultSetAndParamCheck( query, params, errorMsg );
    }

    @Override
    public Set<String> silentlyGetRoleNamesForUser( String username )
    {
        try
        {
            return getRoleNamesForUser( username );
        }
        catch ( InvalidArgumentsException e )
        {
            return Collections.emptySet();
        }
    }

    @Override
    public Set<String> getUsernamesForRole( String roleName ) throws InvalidArgumentsException
    {
        String query = "MATCH (r:Role {name: $role}) OPTIONAL MATCH (u:User)-[:HAS_DB_ROLE]->(:DbRole)-[:FOR_ROLE]->(r) RETURN u.name";
        Map<String,Object> params = map( "role", roleName );
        String errorMsg = "Role '" + roleName + "' does not exist.";

        return systemGraphExecutor.executeQueryWithResultSetAndParamCheck( query, params, errorMsg );
    }

    @Override
    public Set<String> silentlyGetUsernamesForRole( String roleName )
    {
        try
        {
            return getUsernamesForRole( roleName );
        }
        catch ( InvalidArgumentsException e )
        {
            return Collections.emptySet();
        }
    }

    @Override
    public User newUser( String username, String initialPassword, boolean requirePasswordChange ) throws InvalidArgumentsException
    {
        assertValidUsername( username );
        passwordPolicy.validatePassword( initialPassword );

        Credential credential = Credential.forPassword( initialPassword );
        User user = new User.Builder()
                .withName( username )
                .withCredentials( credential )
                .withRequiredPasswordChange( requirePasswordChange )
                .withoutFlag( IS_SUSPENDED )
                .build();

        addUser( user );
        return user;
    }

    private void addUser( User user ) throws InvalidArgumentsException
    {
        String query = "CREATE (u:User {name: $name, credentials: $credentials, passwordChangeRequired: $passwordChangeRequired, suspended: $suspended})";
        Map<String,Object> params =
                MapUtil.map( "name", user.name(),
                        "credentials", serialization.serialize( user.credentials() ),
                        "passwordChangeRequired", user.passwordChangeRequired(),
                        "suspended", user.hasFlag( IS_SUSPENDED ) );
        systemGraphExecutor.executeQueryWithConstraint( query, params,
                "The specified user '" + user.name() + "' already exists." );
    }

    @Override
    public boolean deleteUser( String username ) throws InvalidArgumentsException
    {
        String query = "MATCH (u:User {name: $name}) WITH u, u.name as name DETACH DELETE u RETURN name";
        Map<String,Object> params = map("name", username );
        String errorMsg = "User '" + username + "' does not exist.";

        boolean success = systemGraphExecutor.executeQueryWithParamCheck( query, params, errorMsg );
        clearCacheForUser( username );
        return success;
    }

    @Override
    public User getUser( String username ) throws InvalidArgumentsException
    {
        User[] user = new User[1];

        String query = "MATCH (u:User {name: $name}) RETURN u.credentials, u.passwordChangeRequired, u.suspended";
        Map<String,Object> params = map( "name", username );

        final QueryResult.QueryResultVisitor<FormatException> resultVisitor = row ->
        {
            AnyValue[] fields = row.fields();
            Credential credential = serialization.deserializeCredentials( ((TextValue) fields[0]).stringValue(), 0 );
            boolean requirePasswordChange = ((BooleanValue) fields[1]).booleanValue();
            boolean suspended = ((BooleanValue) fields[2]).booleanValue();

            if ( suspended )
            {
                user[0] = new User.Builder()
                        .withName( username )
                        .withCredentials( credential )
                        .withRequiredPasswordChange( requirePasswordChange )
                        .withFlag( IS_SUSPENDED )
                        .build();
            }
            else
            {
                user[0] = new User.Builder()
                        .withName( username )
                        .withCredentials( credential )
                        .withRequiredPasswordChange( requirePasswordChange )
                        .withoutFlag( IS_SUSPENDED )
                        .build();
            }

            return false;
        };

        systemGraphExecutor.executeQuery( query, params, resultVisitor );

        if ( user[0] == null )
        {
            throw new InvalidArgumentsException( "User '" + username + "' does not exist." );
        }
        return user[0];
    }

    @Override
    public User silentlyGetUser( String username )
    {
        try
        {
            return getUser( username );
        }
        catch ( InvalidArgumentsException e )
        {
            return null;
        }
    }

    @Override
    public void setUserPassword( String username, String password, boolean requirePasswordChange ) throws InvalidArgumentsException
    {
        User existingUser = getUser( username );
        passwordPolicy.validatePassword( password );

        if ( existingUser.credentials().matchesPassword( password ) )
        {
            throw new InvalidArgumentsException( "Old password and new password cannot be the same." );
        }

        String newCredentials = serialization.serialize( Credential.forPassword( password ) );

        String query = "MATCH (u:User {name: $name}) SET u.credentials = $credentials, " +
                "u.passwordChangeRequired = $passwordChangeRequired RETURN u.name";
        Map<String,Object> params =
                map( "name", username,
                        "credentials", newCredentials,
                        "passwordChangeRequired", requirePasswordChange );
        String errorMsg = "User '" + username + "' does not exist.";

        systemGraphExecutor.executeQueryWithParamCheck( query, params, errorMsg );
        clearCacheForUser( username );
    }

    @Override
    public Set<String> getAllUsernames()
    {
        String query = "MATCH (u:User) RETURN u.name";
        return systemGraphExecutor.executeQueryWithResultSet( query );
    }

    private void newDb( String dbName ) throws InvalidArgumentsException
    {
        assertValidDbName( dbName );

        String query = "CREATE (db:Database {name: $dbName})";
        Map<String,Object> params = Collections.singletonMap( "dbName", dbName );

        systemGraphExecutor.executeQueryWithConstraint( query, params, "The specified database '" + dbName + "' already exists." );
    }

    private void assertDbExists( String dbName ) throws InvalidArgumentsException
    {
        String query = "MATCH (db:Database {name: $name}) RETURN db.name";
        Map<String,Object> params = map( "name", dbName );
        String errorMsg = "Database '" + dbName + "' does not exist.";

        systemGraphExecutor.executeQueryWithParamCheck( query, params, errorMsg );
    }

    protected Set<String> getDbNamesForUser( String username ) throws InvalidArgumentsException
    {
        String query = "MATCH (u:User {name: $username}) OPTIONAL MATCH (u)-[:HAS_DB_ROLE]->(:DbRole)-[:FOR_DATABASE]->(db:Database) RETURN db.name";
        Map<String,Object> params = map( "username", username );
        String errorMsg = "User '" + username + "' does not exist.";

        return systemGraphExecutor.executeQueryWithResultSetAndParamCheck( query, params, errorMsg );
    }

    private void ensureDefaultDatabases() throws InvalidArgumentsException
    {
        newDb( DatabaseManager.DEFAULT_DATABASE_NAME );
        newDb( MultiDatabaseManager.SYSTEM_DB_NAME );
    }

    /* Adds neo4j user if no users exist. Adds 'neo4j' to admin role if no admin is assigned. */
    private void ensureDefaultUsers( boolean shouldAssignAdmin ) throws Throwable
    {
        if ( authenticationEnabled || authorizationEnabled )
        {
            String newAdmin = UserManager.INITIAL_USER_NAME;

            // check nbr of users in db
            if ( numberOfUsers() == 0L )
            {
                User initUser;
                if ( initialUserRepository.numberOfUsers() > 0 &&
                        ( initUser = initialUserRepository.getUserByName( UserManager.INITIAL_USER_NAME )) != null )
                {
                    addUser( initUser );
                }
                else
                {
                    newUser( UserManager.INITIAL_USER_NAME, "neo4j", true );
                }

                // Assign the admin role
                addRoleToUser( PredefinedRoles.ADMIN, newAdmin );
            }
            else if ( shouldAssignAdmin )
            {
                Set<String> usernames = getAllUsernames();
                final int numberOfDefaultAdmins = defaultAdminRepository.numberOfUsers();
                if ( numberOfDefaultAdmins > 1 )
                {
                    throw new InvalidArgumentsException(
                            "No roles defined, and multiple users defined as default admin user." +
                                    " Please use `neo4j-admin " + SetDefaultAdminCommand.COMMAND_NAME +
                                    "` to select a valid admin." );
                }
                else if ( numberOfDefaultAdmins == 1 )
                {
                    // We currently support only one default admin
                    String newAdminUsername = defaultAdminRepository.getAllUsernames().iterator().next();
                    if ( getUser( newAdminUsername ) == null )
                    {
                        throw new InvalidArgumentsException(
                                "No roles defined, and default admin user '" + newAdminUsername +
                                        "' does not exist. Please use `neo4j-admin " +
                                        SetDefaultAdminCommand.COMMAND_NAME + "` to select a valid admin." );
                    }
                    newAdmin = newAdminUsername;
                }
                else if ( usernames.size() == 1 )
                {
                    newAdmin = usernames.iterator().next();
                }
                else if ( usernames.contains( UserManager.INITIAL_USER_NAME ) )
                {
                    newAdmin = UserManager.INITIAL_USER_NAME;
                }
                else
                {
                    throw new InvalidArgumentsException(
                            "No roles defined, and cannot determine which user should be admin. " +
                                    "Please use `neo4j-admin " + SetDefaultAdminCommand.COMMAND_NAME +
                                    "` to select an " + "admin." );
                }

                // Assign the admin role
                addRoleToUser( PredefinedRoles.ADMIN, newAdmin );
            }
        }
    }

    /* Builds all predefined roles if no roles exist.
     * Returns true if no roles existed and we had to create them. */
    private boolean ensureDefaultRoles() throws InvalidArgumentsException
    {
        if ( authenticationEnabled || authorizationEnabled )
        {
            if ( numberOfRoles() == 0 )
            {
                for ( String role : PredefinedRolesBuilder.roles.keySet() )
                {
                    newRole( role );
                }
                return true;
            }
        }
        return false;
    }

    private static void assertNotPredefinedRoleName( String roleName ) throws InvalidArgumentsException
    {
        if ( roleName != null && PredefinedRolesBuilder.roles.keySet().contains( roleName ) )
        {
            throw new InvalidArgumentsException(
                    format( "'%s' is a predefined role and can not be deleted.", roleName ) );
        }
    }

    private long numberOfUsers()
    {
        String query = "MATCH (u:User) RETURN count(u)";
        return systemGraphExecutor.executeQueryLong( query );
    }

    private long numberOfRoles()
    {
        String query = "MATCH (r:Role) RETURN count(r)";
        return systemGraphExecutor.executeQueryLong( query );
    }

    // Allow all ascii from '!' to '~', apart from ',' and ':' which are used as separators in flat file
    private static final Pattern usernamePattern = Pattern.compile( "^[\\x21-\\x2B\\x2D-\\x39\\x3B-\\x7E]+$" );

    private void assertValidUsername( String username ) throws InvalidArgumentsException
    {
        if ( username == null || username.isEmpty() )
        {
            throw new InvalidArgumentsException( "The provided username is empty." );
        }
        if ( !usernamePattern.matcher( username ).matches() )
        {
            throw new InvalidArgumentsException(
                    "Username '" + username + "' contains illegal characters. Use ascii characters that are not ',', ':' or whitespaces." );
        }
    }

    private static final Pattern roleNamePattern = Pattern.compile( "^[a-zA-Z0-9_]+$" );

    private void assertValidRoleName( String name ) throws InvalidArgumentsException
    {

        if ( name == null || name.isEmpty() )
        {
            throw new InvalidArgumentsException( "The provided role name is empty." );
        }
        if ( !roleNamePattern.matcher( name ).matches() )
        {
            throw new InvalidArgumentsException( "Role name '" + name + "' contains illegal characters. Use simple ascii characters and numbers." );
        }
    }

    private void assertValidDbName( String name ) throws InvalidArgumentsException
    {
        if ( name == null || name.isEmpty() )
        {
            throw new InvalidArgumentsException( "The provided database name is empty." );
        }
    }

    private void clearCachedAuthorizationInfoForUser( String username )
    {
        clearCachedAuthorizationInfo( new SimplePrincipalCollection( username, this.getName() ) );
    }

    private void clearCacheForUser( String username )
    {
        clearCache( new SimplePrincipalCollection( username, this.getName() ) );
    }

    private void clearCachedAuthorizationInfo()
    {
        Cache<Object, AuthorizationInfo> cache = getAuthorizationCache();
        if ( cache != null )
        {
            cache.clear();
        }
    }
}
