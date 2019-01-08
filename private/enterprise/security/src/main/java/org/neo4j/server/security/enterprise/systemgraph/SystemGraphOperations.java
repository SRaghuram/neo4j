/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.systemgraph;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.neo4j.cypher.result.QueryResult;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.exception.FormatException;
import org.neo4j.server.security.enterprise.auth.SecureHasher;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm.IS_SUSPENDED;

public class SystemGraphOperations
{
    private final QueryExecutor queryExecutor;
    private final SecureHasher secureHasher;

    public SystemGraphOperations( QueryExecutor queryExecutor, SecureHasher secureHasher )
    {
        this.queryExecutor = queryExecutor;
        this.secureHasher = secureHasher;
    }

    void addUser( User user ) throws InvalidArgumentsException
    {
        // NOTE: If username already exists we will violate a constraint
        String query = "CREATE (u:User {name: $name, credentials: $credentials, passwordChangeRequired: $passwordChangeRequired, suspended: $suspended})";
        Map<String,Object> params =
                MapUtil.map( "name", user.name(),
                        "credentials", user.credentials().serialize(),
                        "passwordChangeRequired", user.passwordChangeRequired(),
                        "suspended", user.hasFlag( IS_SUSPENDED ) );
        queryExecutor.executeQueryWithConstraint( query, params,
                "The specified user '" + user.name() + "' already exists." );
    }

    Set<String> getAllUsernames()
    {
        String query = "MATCH (u:User) RETURN u.name";
        return queryExecutor.executeQueryWithResultSet( query );
    }

    AuthorizationInfo doGetAuthorizationInfo( String username )
    {
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

        queryExecutor.executeQuery( query, params, resultVisitor );

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

    void suspendUser( String username ) throws InvalidArgumentsException
    {
        String query = "MATCH (u:User {name: $name}) SET u.suspended = true RETURN 0";
        Map<String,Object> params = map( "name", username );
        String errorMsg = "User '" + username + "' does not exist.";

        queryExecutor.executeQueryWithParamCheck( query, params, errorMsg );
    }

    void activateUser( String username, boolean requirePasswordChange ) throws InvalidArgumentsException
    {
        String query = "MATCH (u:User {name: $name}) SET u.suspended = false, u.passwordChangeRequired = $passwordChangeRequired RETURN 0";
        Map<String,Object> params = map( "name", username, "passwordChangeRequired", requirePasswordChange );
        String errorMsg = "User '" + username + "' does not exist.";

        queryExecutor.executeQueryWithParamCheck( query, params, errorMsg );
    }

    void newRole( String roleName, String... usernames ) throws InvalidArgumentsException
    {
        String query = "CREATE (r:Role {name: $name})";
        Map<String,Object> params = Collections.singletonMap( "name", roleName );

        queryExecutor.executeQueryWithConstraint( query, params,
                "The specified role '" + roleName + "' already exists." );

        // NOTE: This adding users thing is used by tests only so we do not need to optimize this into a more advanced single Cypher query
        for ( String username : usernames )
        {
            addRoleToUser( roleName, username );
        }
    }

    boolean deleteRole( String roleName ) throws InvalidArgumentsException
    {
        String query = "MATCH (r:Role {name: $name}) " +
                "OPTIONAL MATCH (dbr:DbRole)-[:FOR_ROLE]->(r) " +
                "DETACH DELETE r, dbr RETURN 0";

        Map<String,Object> params = map( "name", roleName );
        String errorMsg = "Role '" + roleName + "' does not exist.";

        return queryExecutor.executeQueryWithParamCheck( query, params, errorMsg );
    }

    void assertRoleExists( String roleName ) throws InvalidArgumentsException
    {
        String query = "MATCH (r:Role {name: $name}) RETURN r.name";
        Map<String,Object> params = map( "name", roleName );
        String errorMsg = "Role '" + roleName + "' does not exist.";

        queryExecutor.executeQueryWithParamCheck( query, params, errorMsg );
    }

    @SuppressWarnings( "SameParameterValue" )
    void addRoleToUserForDb( String roleName, String dbName, String username ) throws InvalidArgumentsException
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

        boolean success = queryExecutor.executeQueryWithParamCheck( query, params );

        if ( !success )
        {
            // We need to decide the cause of this failure
            getUser( username, false ); // This throws InvalidArgumentException if user does not exist
            assertRoleExists( roleName ); //This throws InvalidArgumentException if role does not exist
            assertDbExists( dbName ); //This throws InvalidArgument if db does not exist
            // If the user already had the role for the specified database, we should silently fall through
        }
    }

    @SuppressWarnings( "SameParameterValue" )
    void removeRoleFromUserForDb( String roleName, String dbName, String username ) throws InvalidArgumentsException
    {
        assertValidRoleName( roleName );
        assertValidUsername( username );
        assertValidDbName( dbName );

        String query = "MATCH (u:User {name: $name})-[:HAS_DB_ROLE]->(dbr:DbRole)-[:FOR_DATABASE]->(:Database {name: $db}), " +
                "(dbr)-[:FOR_ROLE]->(r:Role {name: $role}) " +
                "DETACH DELETE dbr " +
                "RETURN 0 ";

        Map<String,Object> params = map( "name", username, "role", roleName, "db", dbName );

        boolean success = queryExecutor.executeQueryWithParamCheck( query, params );

        if ( !success )
        {
            // We need to decide the cause of this failure
            getUser( username, false ); // This throws InvalidArgumentException if user does not exist
            assertRoleExists( roleName ); // This throws InvalidArgumentException if role does not exist
            assertDbExists( dbName ); // This throws InvalidArgumentException if db does not exist
            // If the user didn't have the role for the specified db, we should silently fall through
        }
    }

    void addRoleToUser( String roleName, String username ) throws InvalidArgumentsException
    {
        addRoleToUserForDb( roleName, DEFAULT_DATABASE_NAME, username );
    }

    Set<String> getAllRoleNames()
    {
        String query = "MATCH (r:Role) RETURN r.name";
        return queryExecutor.executeQueryWithResultSet( query );
    }

    Set<String> getRoleNamesForUser( String username ) throws InvalidArgumentsException
    {
        String query = "MATCH (u:User {name: $username}) OPTIONAL MATCH (u)-[:HAS_DB_ROLE]->(:DbRole)-[:FOR_ROLE]->(r:Role) RETURN r.name";
        Map<String,Object> params = map( "username", username );
        String errorMsg = "User '" + username + "' does not exist.";

        return queryExecutor.executeQueryWithResultSetAndParamCheck( query, params, errorMsg );
    }

    boolean deleteUser( String username ) throws InvalidArgumentsException
    {
        String query = "MATCH (u:User {name: $name}) " +
                "OPTIONAL MATCH (u)-[:HAS_DB_ROLE]->(dbr :DbRole) " +
                "DETACH DELETE u, dbr RETURN 0";
        Map<String,Object> params = map("name", username );
        String errorMsg = "User '" + username + "' does not exist.";

        return queryExecutor.executeQueryWithParamCheck( query, params, errorMsg );
    }

    Set<String> getUsernamesForRole( String roleName ) throws InvalidArgumentsException
    {
        String query = "MATCH (r:Role {name: $role}) OPTIONAL MATCH (u:User)-[:HAS_DB_ROLE]->(:DbRole)-[:FOR_ROLE]->(r) RETURN u.name";
        Map<String,Object> params = map( "role", roleName );
        String errorMsg = "Role '" + roleName + "' does not exist.";

        return queryExecutor.executeQueryWithResultSetAndParamCheck( query, params, errorMsg );
    }

    User getUser( String username, boolean silent ) throws InvalidArgumentsException
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

        queryExecutor.executeQuery( query, params, resultVisitor );

        if ( user[0] == null && !silent )
        {
            throw new InvalidArgumentsException( "User '" + username + "' does not exist." );
        }

        return user[0];
    }

    void setUserCredentials( String username, String newCredentials, boolean requirePasswordChange ) throws InvalidArgumentsException
    {
        String query = "MATCH (u:User {name: $name}) SET u.credentials = $credentials, " +
                "u.passwordChangeRequired = $passwordChangeRequired RETURN u.name";
        Map<String,Object> params =
                map( "name", username,
                        "credentials", newCredentials,
                        "passwordChangeRequired", requirePasswordChange );
        String errorMsg = "User '" + username + "' does not exist.";

        queryExecutor.executeQueryWithParamCheck( query, params, errorMsg );
    }

    private void assertDbExists( String dbName ) throws InvalidArgumentsException
    {
        String query = "MATCH (db:Database {name: $name}) RETURN db.name";
        Map<String,Object> params = map( "name", dbName );
        String errorMsg = "Database '" + dbName + "' does not exist.";

        queryExecutor.executeQueryWithParamCheck( query, params, errorMsg );
    }

    Set<String> getDbNamesForUser( String username ) throws InvalidArgumentsException
    {
        String query = "MATCH (u:User {name: $username}) OPTIONAL MATCH (u)-[:HAS_DB_ROLE]->(:DbRole)-[:FOR_DATABASE]->(db:Database) RETURN db.name";
        Map<String,Object> params = map( "username", username );
        String errorMsg = "User '" + username + "' does not exist.";

        return queryExecutor.executeQueryWithResultSetAndParamCheck( query, params, errorMsg );
    }

    // Allow all ascii from '!' to '~', apart from ',' and ':' which are used as separators in flat file
    private static final Pattern usernamePattern = Pattern.compile( "^[\\x21-\\x2B\\x2D-\\x39\\x3B-\\x7E]+$" );

    private static void assertValidUsername( String username ) throws InvalidArgumentsException
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

    private static void assertValidRoleName( String name ) throws InvalidArgumentsException
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

    static void assertValidDbName( String name ) throws InvalidArgumentsException
    {
        if ( name == null || name.isEmpty() )
        {
            throw new InvalidArgumentsException( "The provided database name is empty." );
        }
    }
}
