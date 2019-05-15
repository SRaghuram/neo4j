/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.DatabasePrivilege;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.Segment;
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
import org.neo4j.server.security.auth.SecureHasher;
import org.neo4j.server.security.systemgraph.BasicSystemGraphOperations;
import org.neo4j.server.security.systemgraph.QueryExecutor;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.virtual.NodeValue;

import static com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm.assertValidRoleName;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
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

    void newCopyOfRole( String roleName, String from ) throws InvalidArgumentsException
    {
        String query = "MATCH (r:Role {name: $from}) RETURN 0";

        Map<String,Object> params = Collections.singletonMap( "from", from );
        String errorMsg = "Cannot create role '" + roleName + "' from non-existent role '" + from + "'.";

        queryExecutor.executeQueryWithParamCheck( query, params, errorMsg );
        newRole( roleName );
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

    void grantPrivilegeToRole( String roleName, ResourcePrivilege resourcePrivilege ) throws InvalidArgumentsException
    {
        grantPrivilegeToRole( roleName, resourcePrivilege, new DatabasePrivilege() );
    }

    private Map<String,Object> makePrivilegeParameters( String roleName, ResourcePrivilege resourcePrivilege, DatabasePrivilege dbPrivilege )
    {
        assert dbPrivilege.isAllDatabases() || !dbPrivilege.getDbName().isEmpty();
        Resource resource = resourcePrivilege.getResource();
        Map<String,Object> params = map(
                "roleName", roleName,
                "dbName", dbPrivilege.getDbName(),
                "action", resourcePrivilege.getAction().toString(),
                "resource", resource.type().toString(),
                "arg1", resource.getArg1(),
                "arg2", resource.getArg2(),
                "label", resourcePrivilege.getSegment().getLabel()
        );
        return params;
    }

    void grantPrivilegeToRole( String roleName, ResourcePrivilege resourcePrivilege, DatabasePrivilege dbPrivilege ) throws InvalidArgumentsException
    {
        Map<String,Object> params = makePrivilegeParameters( roleName, resourcePrivilege, dbPrivilege );
        boolean fullSegment = resourcePrivilege.getSegment().equals( Segment.ALL );
        String databaseMatch = dbPrivilege.isAllDatabases() ? "MERGE (db:DatabaseAll {name: '*'})" : "MATCH (db:Database {name: $dbName})";
        String qualifierPattern = fullSegment ? "q:LabelQualifierAll {label: '*'}" : "q:LabelQualifier {label: $label}";

        String query = String.format(
                "MATCH (r:Role {name: $roleName}) %s " +
                "MERGE (res:Resource {type: $resource, arg1: $arg1, arg2: $arg2}) " +
                "MERGE (%s) " +
                "MERGE (db)<-[:FOR]-(segment:Segment)-[:QUALIFIED]->(q) " +
                "MERGE (segment)<-[:SCOPE]-(p:Action {action: $action})-[:APPLIES_TO]->(res) " +
                "MERGE (r)-[:GRANTED]->(p) " +
                "RETURN id(p)",
                databaseMatch, qualifierPattern
        );
        assertPrivilegeSuccess( roleName, query, params, dbPrivilege );
    }

    void revokePrivilegeFromRole( String roleName, ResourcePrivilege resourcePrivilege, DatabasePrivilege dbPrivilege ) throws InvalidArgumentsException
    {
        Map<String,Object> params = makePrivilegeParameters( roleName, resourcePrivilege, dbPrivilege );
        boolean fullSegment = resourcePrivilege.getSegment().equals( Segment.ALL );
        String databasePattern = dbPrivilege.isAllDatabases() ? "db:DatabaseAll" : "db:Database {name: $dbName}";
        String qualifierPattern = fullSegment ? "q:LabelQualifierAll" : "q:LabelQualifier {label: label}";

        String query = String.format(
                "MATCH (role:Role)-[g:GRANTED]->(action:Action)-[:APPLIES_TO]->(res:Resource), " +
                "(action)-[:SCOPE]->(segment:Segment), " +
                "(segment)-[:FOR]->(%s), " +
                "(segment)-[:QUALIFIED]-(%s) " +
                "WHERE role.name = $roleName AND action.action = $action AND res.type = $resource AND res.arg1 = $arg1 AND res.arg2 = $arg2 " +
                "DELETE g RETURN 0",
                databasePattern, qualifierPattern
        );
        assertPrivilegeSuccess( roleName, query, params, dbPrivilege );
    }

    private void assertPrivilegeSuccess( String roleName, String query, Map<String,Object> params, DatabasePrivilege dbPrivilege )
            throws InvalidArgumentsException
    {
        if ( !queryExecutor.executeQueryWithParamCheck( query, params ) )
        {
            assertRoleExists( roleName );
            if ( !dbPrivilege.isAllDatabases() )
            {
                assertDbExists( dbPrivilege.getDbName() );
            }
        }
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
                "MATCH (r:Role)-[:GRANTED]->(a:Action)-[:SCOPE]->(segment:Segment), " +
                "(a)-[:APPLIES_TO]->(res) " +
                "WHERE r.name IN $roles " +
                "MATCH (segment)-[:FOR]->(db) " +
                "MATCH (segment)-[:QUALIFIED]->(q) " +
                "RETURN db.name, db, a.action, res, q";

        Map<String, DatabasePrivilege> results = new HashMap<>();

        final QueryResult.QueryResultVisitor<RuntimeException> resultVisitor = row ->
        {
            AnyValue dbNameValue = row.fields()[0];
            NodeValue database = (NodeValue) row.fields()[1];
            assert database.labels().length() == 1;
            DatabasePrivilege dbpriv;
            if ( database.labels().stringValue( 0 ).equals( "Database" ) )
            {
                String dbName = ((TextValue) dbNameValue).stringValue();
                dbpriv = results.computeIfAbsent( dbName, DatabasePrivilege::new );
            }
            else if ( database.labels().stringValue( 0 ).equals( "DatabaseAll" ) )
            {
                dbpriv = results.computeIfAbsent( "", db -> new DatabasePrivilege() );
            }
            else
            {
                throw new IllegalStateException( "Cannot have database node without either 'Database' or 'DatabaseAll' labels: " + database.labels() );
            }

            String actionValue = ((TextValue) row.fields()[2]).stringValue();
            NodeValue resource = (NodeValue) row.fields()[3];
            NodeValue qualifier = (NodeValue) row.fields()[4];
            try
            {
                dbpriv.addPrivilege( PrivilegeBuilder.grant( actionValue )
                        .withinScope( qualifier )
                        .onResource( resource )
                        .build() );
            }
            catch ( InvalidArgumentsException ie )
            {
                throw new IllegalStateException( "Failed to authorize", ie );
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
}
