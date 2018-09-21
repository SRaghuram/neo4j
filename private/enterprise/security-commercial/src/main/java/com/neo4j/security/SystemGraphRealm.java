/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.DisabledAccountException;
import org.apache.shiro.authc.ExcessiveAttemptsException;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.apache.shiro.authc.UnknownAccountException;
import org.apache.shiro.authc.pam.UnsupportedTokenException;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.crypto.hash.SimpleHash;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.neo4j.commandline.admin.security.SetDefaultAdminCommand;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthProviderFailedException;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.PasswordPolicy;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.ListSnapshot;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.auth.exception.FormatException;
import org.neo4j.server.security.enterprise.auth.EnterpriseUserManager;
import org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder;
import org.neo4j.server.security.enterprise.auth.RealmLifecycle;
import org.neo4j.server.security.enterprise.auth.RoleRecord;
import org.neo4j.server.security.enterprise.auth.RoleRepository;
import org.neo4j.server.security.enterprise.auth.SecureHasher;
import org.neo4j.server.security.enterprise.auth.ShiroAuthToken;
import org.neo4j.server.security.enterprise.auth.ShiroAuthenticationInfo;
import org.neo4j.server.security.enterprise.auth.ShiroAuthorizationInfoProvider;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.server.security.enterprise.log.SecurityLog;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static com.neo4j.security.CommercialSecurityModule.IMPORT_AUTH_COMMAND_NAME;
import static java.lang.String.format;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * Shiro realm using a Neo4j graph to store users and roles
 */
class SystemGraphRealm extends AuthorizingRealm implements RealmLifecycle, EnterpriseUserManager, ShiroAuthorizationInfoProvider
{
    private final SystemGraphImportOptions importOptions;
    private final PasswordPolicy passwordPolicy;
    private final AuthenticationStrategy authenticationStrategy;
    private final boolean authenticationEnabled;
    private final boolean authorizationEnabled;
    private final SecureHasher secureHasher;
    private final SystemGraphExecutor systemGraphExecutor;
    private final SecurityLog securityLog;

    /**
     * This flag is used in the same way as User.PASSWORD_CHANGE_REQUIRED, but it's
     * placed here because of user suspension not being a part of community edition
     */
    private static final String IS_SUSPENDED = "is_suspended";

    SystemGraphRealm( SystemGraphExecutor systemGraphExecutor, SecureHasher secureHasher, PasswordPolicy passwordPolicy,
            AuthenticationStrategy authenticationStrategy, boolean authenticationEnabled, boolean authorizationEnabled,
            SecurityLog securityLog, SystemGraphImportOptions importOptions )
    {
        super();

        setName( SecuritySettings.SYSTEM_GRAPH_REALM_NAME );

        this.secureHasher = secureHasher;
        this.passwordPolicy = passwordPolicy;
        this.authenticationStrategy = authenticationStrategy;
        this.authenticationEnabled = authenticationEnabled;
        this.authorizationEnabled = authorizationEnabled;
        this.securityLog = securityLog;
        this.importOptions = importOptions;
        this.systemGraphExecutor = systemGraphExecutor;
        setAuthenticationCachingEnabled( true );
        setAuthorizationCachingEnabled( true );
        setCredentialsMatcher( secureHasher.getHashedCredentialsMatcher() );
        setRolePermissionResolver( PredefinedRolesBuilder.rolePermissionResolver );
    }

    @Override
    public void initialize() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
        if ( authenticationEnabled || authorizationEnabled )
        {
            initializeSystemGraph();
        }
    }

    @Override
    public void stop() throws Throwable
    {
    }

    @Override
    public void shutdown() throws Throwable
    {
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
        case SUCCESS:
            break;
        case PASSWORD_CHANGE_REQUIRED:
            break;
        case FAILURE:
            throw new IncorrectCredentialsException();
        case TOO_MANY_ATTEMPTS:
            throw new ExcessiveAttemptsException();
        default:
            throw new AuthenticationException();
        }

        if ( user.hasFlag( IS_SUSPENDED ) )
        {
            throw new DisabledAccountException( "User '" + user.name() + "' is suspended." );
        }

        if ( user.passwordChangeRequired() )
        {
            result = AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
        }

        // Extract the hashed credentials so that it can be cached in the authentication cache
        SystemGraphCredential existingCredentials = (SystemGraphCredential) user.credentials();
        SimpleHash hashedCredentials = existingCredentials.hashedCredentials();

        return new ShiroAuthenticationInfo( user.name(), hashedCredentials.getBytes(),
                hashedCredentials.getSalt(), getName() /* Realm name */, result );
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

        MutableBoolean existingUser = new MutableBoolean( false );
        MutableBoolean passwordChangeRequired = new MutableBoolean( false );
        MutableBoolean suspended = new MutableBoolean( false );
        Set<String> roleNames = new TreeSet<>();

        String query = "MATCH (u:User {name: $username}) OPTIONAL MATCH (u)-[:HAS_DB_ROLE]->(:DbRole)-[:FOR_ROLE]->(r:Role) " +
                "RETURN u.passwordChangeRequired, u.suspended, r.name";
        Map<String,Object> params = map( "username", username );

        final QueryResult.QueryResultVisitor<RuntimeException> resultVisitor = row ->
        {
            AnyValue[] fields = row.fields();
            existingUser.setTrue();
            passwordChangeRequired.setValue( ((BooleanValue) fields[0]).booleanValue() );
            suspended.setValue( ((BooleanValue) fields[1]).booleanValue() );

            Value role = (Value) fields[2];
            if ( role != Values.NO_VALUE )
            {
                roleNames.add( ((TextValue) role).stringValue() );
            }

            return true;
        };

        systemGraphExecutor.executeQuery( query, params, resultVisitor );

        if ( existingUser.isFalse() )
        {
            return null;
        }

        if ( passwordChangeRequired.isTrue() || suspended.isTrue() )
        {
            return new SimpleAuthorizationInfo();
        }

        return new SimpleAuthorizationInfo( roleNames );
    }

    @Override
    protected Object getAuthorizationCacheKey( PrincipalCollection principals )
    {
        return getAvailablePrincipal( principals );
    }

    @Override
    public AuthorizationInfo getAuthorizationInfoSnapshot( PrincipalCollection principalCollection )
    {
        return getAuthorizationInfo( principalCollection );
    }

    @Override
    public void suspendUser( String username ) throws InvalidArgumentsException
    {
        String query = "MATCH (u:User {name: $name}) SET u.suspended = true RETURN 0";
        Map<String,Object> params = map( "name", username );
        String errorMsg = "User '" + username + "' does not exist.";

        systemGraphExecutor.executeQueryWithParamCheck( query, params, errorMsg );
        clearCacheForUser( username );
    }

    @Override
    public void activateUser( String username, boolean requirePasswordChange ) throws InvalidArgumentsException
    {
        String query = "MATCH (u:User {name: $name}) SET u.suspended = false, u.passwordChangeRequired = $passwordChangeRequired RETURN 0";
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
        return doDeleteRole( roleName );
    }

    private boolean doDeleteRole( String roleName ) throws InvalidArgumentsException
    {
        String query = "MATCH (r:Role {name: $name}) " +
                "OPTIONAL MATCH (dbr:DbRole)-[:FOR_ROLE]->(r) " +
                "DETACH DELETE r, dbr RETURN 0";

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
        addRoleToUserForDb( roleName, DEFAULT_DATABASE_NAME, username );
    }

    private void addRoleToUserForDb( String roleName, String dbName, String username ) throws InvalidArgumentsException
    {
        assertValidRoleName( roleName );
        assertValidUsername( username );
        assertValidDbName( dbName );

        String query = "MATCH (u:User {name: $user}), (r:Role {name: $role}), (d:Database {name: $db}) " +
                "OPTIONAL MATCH (u)-[:HAS_DB_ROLE]->(dbr:DbRole)-[:FOR_DATABASE]->(d), (dbr)-[:FOR_ROLE]->(r) " +
                "WITH u, r, d WHERE dbr IS NULL " +
                "CREATE (newDbr:DbRole)-[:FOR_ROLE]->(r) " +
                "CREATE (u)-[:HAS_DB_ROLE]->(newDbr)-[:FOR_DATABASE]->(d) " +
                "RETURN 0";
        Map<String,Object> params = map("user", username, "role", roleName, "db", dbName);

        boolean success = systemGraphExecutor.executeQueryWithParamCheck( query, params );

        if ( !success )
        {
            // We need to decide the cause of this failure
            getUser( username ); // This throws InvalidArgumentException if user does not exist
            assertRoleExists( roleName ); //This throws InvalidArgumentException if role does not exist
            assertDbExists( dbName ); //This throws InvalidArgument if db does not exist
            // If the user already had the role for the specified database, we should silently fall through
        }

        clearCachedAuthorizationInfoForUser( username );
    }

    @Override
    public void removeRoleFromUser( String roleName, String username ) throws InvalidArgumentsException
    {
        removeRoleFromUserForDb( roleName, DEFAULT_DATABASE_NAME, username );
    }

    private void removeRoleFromUserForDb( String roleName, String dbName, String username ) throws InvalidArgumentsException
    {
        assertValidRoleName( roleName );
        assertValidUsername( username );
        assertValidDbName( dbName );

        String query = "MATCH (u:User {name: $name})-[:HAS_DB_ROLE]->(dbr:DbRole)-[:FOR_DATABASE]->(:Database {name: $db}), " +
                "(dbr)-[:FOR_ROLE]->(r:Role {name: $role}) " +
                "DETACH DELETE dbr " +
                "RETURN 0 ";

        Map<String,Object> params = map( "name", username, "role", roleName, "db", dbName);

        boolean success = systemGraphExecutor.executeQueryWithParamCheck( query, params );

        if ( !success )
        {
            // We need to decide the cause of this failure
            getUser( username ); // This throws InvalidArgumentException if user does not exist
            assertRoleExists( roleName ); // This throws InvalidArgumentException if role does not exist
            assertDbExists( dbName ); // This throws InvalidArgumentException if db does not exist
            // If the user didn't have the role for the specified db, we should silently fall through
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

        Credential credential = createCredentialForPassword( initialPassword );
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
        // NOTE: If username already exists we will violate a constraint
        String query = "CREATE (u:User {name: $name, credentials: $credentials, passwordChangeRequired: $passwordChangeRequired, suspended: $suspended})";
        Map<String,Object> params =
                MapUtil.map( "name", user.name(),
                        "credentials", user.credentials().serialize(),
                        "passwordChangeRequired", user.passwordChangeRequired(),
                        "suspended", user.hasFlag( IS_SUSPENDED ) );
        systemGraphExecutor.executeQueryWithConstraint( query, params,
                "The specified user '" + user.name() + "' already exists." );
    }

    @Override
    public boolean deleteUser( String username ) throws InvalidArgumentsException
    {
        String query = "MATCH (u:User {name: $name}) " +
                "OPTIONAL MATCH (u)-[:HAS_DB_ROLE]->(dbr :DbRole) " +
                "DETACH DELETE u, dbr RETURN 0";
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
            Credential credential = SystemGraphCredential.deserialize( ((TextValue) fields[0]).stringValue(), secureHasher );
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

        String newCredentials = SystemGraphCredential.serialize( createCredentialForPassword( password ) );

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

    private SystemGraphCredential createCredentialForPassword( String password )
    {
        SimpleHash hash = secureHasher.hash( password.getBytes() );
        return new SystemGraphCredential( secureHasher, hash );
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

    private void initializeSystemGraph() throws Throwable
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
            systemGraphExecutor.executeQuery( "CREATE CONSTRAINT ON (u:User) ASSERT u.name IS UNIQUE", Collections.emptyMap(), resultVisitor );
            systemGraphExecutor.executeQuery( "CREATE CONSTRAINT ON (r:Role) ASSERT r.name IS UNIQUE", Collections.emptyMap(), resultVisitor );
            systemGraphExecutor.executeQuery( "CREATE CONSTRAINT ON (d:Database) ASSERT d.name IS UNIQUE", Collections.emptyMap(), resultVisitor );

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
        // default predefined roles and users and make sure we have an admin user
        ensureDefaultUsersAndRoles();
    }

    private boolean isSystemGraphEmpty()
    {
        // Execute a query to see if the system database exists
        String query = "MATCH (db:Database {name: $name}) RETURN db.name";
        Map<String,Object> params = map( "name", SYSTEM_DATABASE_NAME );

        return !systemGraphExecutor.executeQueryWithParamCheck( query, params );
    }

    private void ensureDefaultUsersAndRoles() throws Throwable
    {
        Set<String> addedDefaultUsers = ensureDefaultUsers();
        ensureDefaultRoles( addedDefaultUsers );
    }

    private void ensureDefaultDatabases() throws InvalidArgumentsException
    {
        newDb( DEFAULT_DATABASE_NAME );
        newDb( SYSTEM_DATABASE_NAME );
    }

    private UserRepository startUserRepository( Supplier<UserRepository> supplier ) throws Throwable
    {
        UserRepository userRepository = supplier.get();
        userRepository.init();
        userRepository.start();
        return userRepository;
    }

    private void stopUserRepository( UserRepository userRepository ) throws Throwable
    {
        userRepository.stop();
        userRepository.shutdown();
    }

    private RoleRepository startRoleRepository( Supplier<RoleRepository> supplier ) throws Throwable
    {
        RoleRepository roleRepository = supplier.get();
        roleRepository.init();
        roleRepository.start();
        return roleRepository;
    }

    private void stopRoleRepository( RoleRepository roleRepository ) throws Throwable
    {
        roleRepository.stop();
        roleRepository.shutdown();
    }

    /* Adds neo4j user if no users exist */
    private Set<String> ensureDefaultUsers() throws Throwable
    {
        if ( numberOfUsers() == 0 )
        {
            Set<String> addedUsernames = new TreeSet<>();
            if ( importOptions.initialUserRepositorySupplier != null )
            {
                UserRepository initialUserRepository = startUserRepository( importOptions.initialUserRepositorySupplier );
                if ( initialUserRepository.numberOfUsers() > 0 )
                {
                    // In alignment with InternalFlatFileRealm we only allow the INITIAL_USER_NAME here for now
                    // (This is what we get from the `set-initial-password` command)
                    User initialUser = initialUserRepository.getUserByName( INITIAL_USER_NAME );
                    if ( initialUser != null )
                    {
                        addUser( initialUser );
                        addedUsernames.add( initialUser.name() );
                    }
                }
                stopUserRepository( initialUserRepository );
            }

            // If no initial user was set create the default neo4j user
            if ( addedUsernames.isEmpty() )
            {
                newUser( INITIAL_USER_NAME, "neo4j", true );
                addedUsernames.add( INITIAL_USER_NAME );
            }

            return addedUsernames;
        }
        return Collections.emptySet();
    }

    /* Builds all predefined roles if no roles exist. Adds 'neo4j' to admin role if no admin is assigned */
    private void ensureDefaultRoles( Set<String> addedDefaultUsers ) throws Throwable
    {
        List<String> newAdmins = new LinkedList<>( addedDefaultUsers );

        if ( numberOfRoles() == 0 )
        {
            if ( newAdmins.isEmpty() )
            {
                String newAdminUsername = null;

                // Try to import the name of a single admin user as set by the SetDefaultAdmin command
                if ( importOptions.defaultAdminRepositorySupplier != null )
                {
                    UserRepository defaultAdminRepository = startUserRepository( importOptions.defaultAdminRepositorySupplier );
                    final int numberOfDefaultAdmins = defaultAdminRepository.numberOfUsers();
                    if ( numberOfDefaultAdmins > 1 )
                    {
                        throw new InvalidArgumentsException(
                                "No roles defined, and multiple users defined as default admin user." +
                                        " Please use `neo4j-admin " + SetDefaultAdminCommand.COMMAND_NAME +
                                        "` to select a valid admin." );
                    }
                    newAdminUsername = numberOfDefaultAdmins == 0 ? null :
                                       defaultAdminRepository.getAllUsernames().iterator().next();
                    stopUserRepository( defaultAdminRepository );
                }

                Set<String> usernames = getAllUsernames();

                if ( newAdminUsername != null )
                {
                    // We currently support only one default admin
                    if ( silentlyGetUser( newAdminUsername ) == null )
                    {
                        throw new InvalidArgumentsException(
                                "No roles defined, and default admin user '" + newAdminUsername +
                                        "' does not exist. Please use `neo4j-admin " +
                                        SetDefaultAdminCommand.COMMAND_NAME + "` to select a valid admin." );
                    }
                    newAdmins.add( newAdminUsername );
                }
                else if ( usernames.size() == 1 )
                {
                    // If only a single user exists, make her an admin
                    newAdmins.add( usernames.iterator().next() );
                }
                else if ( usernames.contains( INITIAL_USER_NAME ) )
                {
                    // If the default neo4j user exists, make her an admin
                    newAdmins.add( INITIAL_USER_NAME );
                }
                else
                {
                    throw new InvalidArgumentsException(
                            "No roles defined, and cannot determine which user should be admin. " +
                                    "Please use `neo4j-admin " + SetDefaultAdminCommand.COMMAND_NAME +
                                    "` to select an " + "admin." );
                }
            }

            // Create the predefined roles
            for ( String role : PredefinedRolesBuilder.roles.keySet() )
            {
                newRole( role );
            }
        }

        // Actually assign the admin role
        for ( String username : newAdmins )
        {
            addRoleToUser( PredefinedRoles.ADMIN, username );
            securityLog.info( "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, username );
        }
    }

    private void migrateFromFlatFileRealm() throws Throwable
    {
        UserRepository userRepository = startUserRepository( importOptions.migrationUserRepositorySupplier );
        RoleRepository roleRepository = startRoleRepository( importOptions.migrationRoleRepositorySupplier );

        if ( !doImportUsersAndRoles( userRepository, roleRepository, /* !purgeOnSuccess */ false ) )
        {
            throw new InvalidArgumentsException(
                    "Automatic migration of users and roles into system graph failed because repository files are inconsistent. " +
                            "Please use `neo4j-admin " + IMPORT_AUTH_COMMAND_NAME + "` to perform migration manually." );
        }

        stopUserRepository( userRepository );
        stopRoleRepository( roleRepository );
    }

    private void importUsersAndRoles() throws Throwable
    {
        UserRepository userRepository = startUserRepository( importOptions.importUserRepositorySupplier );
        RoleRepository roleRepository = startRoleRepository( importOptions.importRoleRepositorySupplier );

        if ( !doImportUsersAndRoles( userRepository, roleRepository, importOptions.shouldPurgeImportRepositoriesAfterSuccesfulImport ) )
        {
            throw new InvalidArgumentsException(
                    "Import of users and roles into system graph failed because the import files are inconsistent. " +
                            "Please use `neo4j-admin " + IMPORT_AUTH_COMMAND_NAME + "` to retry import again." );
        }

        stopUserRepository( userRepository );
        stopRoleRepository( roleRepository );
    }

    private boolean doImportUsersAndRoles( UserRepository userRepository, RoleRepository roleRepository, boolean purgeOnSuccess ) throws Throwable
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

            try ( Transaction transaction = systemGraphExecutor.systemDbBeginTransaction() )
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
                    addUser( user );
                }
                for ( RoleRecord role : roles.values() )
                {
                    newRole( role.name() );
                    for ( String username : role.users() )
                    {
                        addRoleToUser( role.name(), username );
                    }
                }
                transaction.success();
            }
            catch ( Throwable t )
            {
                throw t;
            }

            assert validateImportSucceeded( userRepository, roleRepository );

            // Log what happened to the security log
            if ( importOptions.shouldResetSystemGraphAuthBeforeImport )
            {
                String userString = numberOfDeletedUsersAndRoles.first() == 1 ? "user" : "users";
                String roleString = numberOfDeletedUsersAndRoles.other() == 1 ? "role" : "roles";

                securityLog.info( "Deleted %s %s and %s %s into system graph.",
                        Integer.toString( numberOfDeletedUsersAndRoles.first() ), userString,
                        Integer.toString( numberOfDeletedUsersAndRoles.other() ), roleString );
            }
            {
                String userString = users.values().size() == 1 ? "user" : "users";
                String roleString = roles.values().size() == 1 ? "role" : "roles";

                securityLog.info( "Completed import of %s %s and %s %s into system graph.",
                        Integer.toString( users.values().size() ), userString,
                        Integer.toString( roles.values().size() ), roleString );
            }

        }

        // If transaction succeeded, we purge the repositories so that we will not try to import them again the next time we restart
        if ( purgeOnSuccess )
        {
            userRepository.purge();
            roleRepository.purge();

            securityLog.debug( "Source import user and role repositories were purged." );
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

        Set<String> usernames = getAllUsernames();
        for ( String username : usernames )
        {
            deleteUser( username );
        }

        Set<String> roleNames = getAllRoleNames();
        for ( String roleName : roleNames )
        {
            doDeleteRole( roleName );
        }

        // TODO: Delete Database nodes? (Only if they are exclusively used by the security module)

        return Pair.of( usernames.size(), roleNames.size() );
    }

    private boolean validateImportSucceeded( UserRepository userRepository, RoleRepository roleRepository ) throws Throwable
    {
        // Take a new snapshot of the import repositories
        ListSnapshot<User> users = userRepository.getPersistedSnapshot();
        ListSnapshot<RoleRecord> roles = roleRepository.getPersistedSnapshot();

        try ( Transaction transaction = systemGraphExecutor.systemDbBeginTransaction() )
        {

            Set<String> systemGraphUsers = getAllUsernames();
            List<String> repoUsernames = users.values().stream().map( u -> u.name() ).collect( Collectors.toList() );
            if ( !systemGraphUsers.containsAll( repoUsernames ) )
            {
                throw new IOException( "Users were not imported correctly" );
            }

            List<String> repoRoleNames = roles.values().stream().map( r -> r.name() ).collect( Collectors.toList() );
            Set<String> systemGraphRoles = getAllRoleNames();
            if ( !systemGraphRoles.containsAll( repoRoleNames ) )
            {
                throw new IOException( "Roles were not imported correctly" );
            }

            for ( RoleRecord role : roles.values() )
            {
                Set<String> usernamesForRole = getUsernamesForRole( role.name() );
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
