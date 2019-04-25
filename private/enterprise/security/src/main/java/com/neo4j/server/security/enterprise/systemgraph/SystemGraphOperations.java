/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.DatabasePrivilege;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import org.neo4j.server.security.auth.SecureHasher;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.neo4j.cypher.result.QueryResult;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.server.security.systemgraph.BasicSystemGraphOperations;
import org.neo4j.server.security.systemgraph.QueryExecutor;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;

import static com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm.assertValidRoleName;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.security.systemgraph.BasicSystemGraphRealm.assertValidUsername;
import static org.neo4j.values.storable.Values.NO_VALUE;

public class SystemGraphOperations extends BasicSystemGraphOperations
{
    public SystemGraphOperations( QueryExecutor queryExecutor, SecureHasher secureHasher )
    {
        super( queryExecutor, secureHasher );
    }

    AuthorizationInfo doGetAuthorizationInfo( String username )
    {
        MutableBoolean existingUser = new MutableBoolean( false );
        MutableBoolean passwordChangeRequired = new MutableBoolean( false );
        MutableBoolean suspended = new MutableBoolean( false );
        Set<String> roleNames = new TreeSet<>();

        String query =
                "MATCH (u:User {name: $username}) " +
                "OPTIONAL MATCH (u)-[:HAS_ROLE]->(r:Role) " +
                "RETURN u.passwordChangeRequired, u.suspended, r.name";

        Map<String,Object> params = map( "username", username );

        final QueryResult.QueryResultVisitor<RuntimeException> resultVisitor = row ->
        {
            AnyValue[] fields = row.fields();
            existingUser.setTrue();
            passwordChangeRequired.setValue( ((BooleanValue) fields[0]).booleanValue() );
            suspended.setValue( ((BooleanValue) fields[1]).booleanValue() );

            Value role = (Value) fields[2];
            if ( role != NO_VALUE )
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
        String query = "MATCH (r:Role {name: $name}) DETACH DELETE r RETURN 0";

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
    void removeRoleFromUser( String roleName, String username ) throws InvalidArgumentsException
    {
        assertValidRoleName( roleName );
        assertValidUsername( username );

        String query = "MATCH (u:User {name: $name})-[dbr:HAS_ROLE]->(r:Role {name: $role}) " +
                "DELETE dbr " +
                "RETURN 0 ";

        Map<String,Object> params = map( "name", username, "role", roleName );

        boolean success = queryExecutor.executeQueryWithParamCheck( query, params );

        if ( !success )
        {
            // We need to decide the cause of this failure
            getUser( username, false ); // This throws InvalidArgumentException if user does not exist
            assertRoleExists( roleName ); // This throws InvalidArgumentException if role does not exist
            // If the user didn't have the role for the specified db, we should silently fall through
        }
    }

    void addRoleToUser( String roleName, String username ) throws InvalidArgumentsException
    {
        assertValidRoleName( roleName );
        assertValidUsername( username );

        String query =
                "MATCH (u:User {name: $user}), (r:Role {name: $role}) " +
                "OPTIONAL MATCH (u)-[h:HAS_ROLE]->(r) " +
                "WITH u, r WHERE h IS NULL " +
                "CREATE (u)-[:HAS_ROLE]->(r) " +
                "RETURN 0";
        Map<String,Object> params = map( "user", username, "role", roleName );

        boolean success = queryExecutor.executeQueryWithParamCheck( query, params );

        if ( !success )
        {
            // We need to decide the cause of this failure
            getUser( username, false ); // This throws InvalidArgumentException if user does not exist
            assertRoleExists( roleName ); //This throws InvalidArgumentException if role does not exist
            // If the user already had the role, we should silently fall through
        }
    }

    void grantPrivilegeToRole( String roleName, ResourcePrivilege resourcePrivilege, String dbName ) throws InvalidArgumentsException
    {
        Map<String,Object> params = map(
                "roleName", roleName,
                "action", resourcePrivilege.getAction().toString(),
                "resource", resourcePrivilege.getResource().toString(),
                "dbName", dbName
        );

        String query =
                "MERGE (res:Resource {type: $resource}) WITH res " +
                "MATCH (r:Role {name: $roleName}), (db:Database {name: $dbName}) " +
                "MERGE (r)-[:GRANTED]->(p:Privilege {action: $action})-[:APPLIES_TO]->(res) " +
                "MERGE (p)-[:SCOPE]->(db) RETURN 0";
        boolean success = queryExecutor.executeQueryWithParamCheck( query, params );

        if ( !success )
        {
            assertRoleExists( roleName ); // This throws InvalidArgumentException if role does not exist
            assertDbExists( dbName );
        }
    }

    void revokePrivilegeFromRole( String roleName, ResourcePrivilege resourcePrivilege, String dbName ) throws InvalidArgumentsException
    {
        assertRoleExists( roleName ); // This throws InvalidArgumentException if role does not exist
        Map<String,Object> params = map(
                "roleName", roleName,
                "action", resourcePrivilege.getAction().toString(),
                "resource", resourcePrivilege.getResource().toString(),
                "dbName", dbName
        );
        String query =
                "MATCH (role:Role)-[:GRANTED]->(p:Privilege)-[:APPLIES_TO]->(res:Resource), (p)-[:SCOPE]->(db:Database) " +
                "WHERE role.name = $roleName AND p.action = $action AND res.type = $resource AND db.name = $dbName " +
                "DETACH DELETE p RETURN 0";
        queryExecutor.executeQueryWithParamCheck( query, params );
    }

    Set<DatabasePrivilege> showPrivilegesForUser( String username ) throws InvalidArgumentsException
    {
        getUser( username, false );
        Set<String> roles = getRoleNamesForUser( username );
        return getPrivilegeForRoles( roles );
    }

    Set<DatabasePrivilege> getPrivilegeForRoles( Set<String> roles )
    {
        String query =
                "MATCH (r:Role)-[:GRANTED]->(p:Privilege)-[:SCOPE]->(db:Database) " +
                "WHERE r.name IN $roles " +
                "OPTIONAL MATCH (p)-[:APPLIES_TO]->(res) " +
                "RETURN db.name AS dbname, p.action AS action, res.type AS resource ORDER BY resource";

        Map<String, DatabasePrivilege> results = new HashMap<>();

        final QueryResult.QueryResultVisitor<RuntimeException> resultVisitor = row ->
        {
            AnyValue dbNameValue = row.fields()[0];
            String dbName = "*";
            if ( dbNameValue != NO_VALUE )
            {
                dbName = ((TextValue) dbNameValue).stringValue();
            }

            DatabasePrivilege dbpriv = results.getOrDefault( dbName, new DatabasePrivilege( dbName ) );

            AnyValue actionValue = row.fields()[1];
            AnyValue resourceValue = row.fields()[2];
            if ( actionValue != NO_VALUE && resourceValue != NO_VALUE )
            {
                String action = ((TextValue) actionValue).stringValue();
                String resource = ((TextValue) resourceValue).stringValue();
                try
                {
                    ResourcePrivilege privilege = new ResourcePrivilege( action, resource );
                    dbpriv.addPrivilege( privilege );
                    results.put( dbName, dbpriv );
                }
                catch ( InvalidArgumentsException ignored )
                {
                }
            }
            return true;
        };

        queryExecutor.executeQuery( query, Collections.singletonMap( "roles", roles ), resultVisitor );

        return new HashSet<>( results.values() );
    }

    Set<String> getAllRoleNames()
    {
        String query = "MATCH (r:Role) RETURN r.name";
        return queryExecutor.executeQueryWithResultSet( query );
    }

    Set<String> getRoleNamesForUser( String username ) throws InvalidArgumentsException
    {
        String query = "MATCH (u:User {name: $username}) OPTIONAL MATCH (u)-[:HAS_ROLE]->(r:Role) RETURN r.name";
        Map<String,Object> params = map( "username", username );
        String errorMsg = "User '" + username + "' does not exist.";

        return queryExecutor.executeQueryWithResultSetAndParamCheck( query, params, errorMsg );
    }

    Set<String> getUsernamesForRole( String roleName ) throws InvalidArgumentsException
    {
        String query = "MATCH (r:Role {name: $role}) OPTIONAL MATCH (u:User)-[:HAS_ROLE]->(r) RETURN u.name";
        Map<String,Object> params = map( "role", roleName );
        String errorMsg = "Role '" + roleName + "' does not exist.";

        return queryExecutor.executeQueryWithResultSetAndParamCheck( query, params, errorMsg );
    }

    private void assertDbExists( String dbName ) throws InvalidArgumentsException
    {
        String query = "MATCH (db:Database {name: $name}) RETURN db.name";
        Map<String,Object> params = map( "name", dbName );
        String errorMsg = "Database '" + dbName + "' does not exist.";

        queryExecutor.executeQueryWithParamCheck( query, params, errorMsg );
    }

    static void assertValidDbName( String name ) throws InvalidArgumentsException
    {
        if ( name == null || name.isEmpty() )
        {
            throw new InvalidArgumentsException( "The provided database name is empty." );
        }
    }
}
