/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.neo4j.server.security.enterprise.auth.LabelSegment;
import com.neo4j.server.security.enterprise.auth.RelTypeSegment;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.Segment;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ForkJoinPool;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.server.security.auth.SecureHasher;
import org.neo4j.server.security.systemgraph.BasicSystemGraphOperations;
import org.neo4j.server.security.systemgraph.ErrorPreservingQuerySubscriber;
import org.neo4j.server.security.systemgraph.QueryExecutor;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.NodeValue;

import static com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm.assertValidRoleName;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.server.security.systemgraph.BasicSystemGraphRealm.assertValidUsername;
import static org.neo4j.values.storable.Values.NO_VALUE;

public class SystemGraphOperations extends BasicSystemGraphOperations
{
    private com.github.benmanes.caffeine.cache.Cache<String,Set<ResourcePrivilege>> privilegeCache;

    public SystemGraphOperations( QueryExecutor queryExecutor, SecureHasher secureHasher )
    {
        super( queryExecutor, secureHasher );
        Caffeine<Object,Object> builder = Caffeine.newBuilder()
                .maximumSize( 10000 )
                .executor( ForkJoinPool.commonPool() )
                .expireAfterAccess( Duration.ofHours( 1 ) );
        privilegeCache = builder.build();
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

        final ErrorPreservingQuerySubscriber subscriber = new ErrorPreservingQuerySubscriber()
        {
            @Override
            public void onField( int offset, AnyValue value )
            {
                switch ( offset )
                {
                case 0://u.passwordChangeRequired
                    existingUser.setTrue();
                    passwordChangeRequired.setValue( ((BooleanValue) value).booleanValue() );
                    break;
                case 1://u.suspended
                    suspended.setValue( ((BooleanValue) value).booleanValue() );
                    break;
                case 2://r.name
                    if ( value != NO_VALUE )
                    {
                        roleNames.add( ((TextValue) value).stringValue() );
                    }
                    break;
                default://nothing to do
                }
            }
        };

        queryExecutor.executeQuery( query, params, subscriber );

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

    private Map<String,Object> makePrivilegeParameters( String roleName, ResourcePrivilege resourcePrivilege )
    {
        assert resourcePrivilege.isAllDatabases() || !resourcePrivilege.getDbName().isEmpty();
        Resource resource = resourcePrivilege.getResource();
        return map(
                "roleName", roleName,
                "dbName", resourcePrivilege.getDbName(),
                "action", resourcePrivilege.getAction().toString(),
                "resource", resource.type().toString(),
                "arg1", resource.getArg1(),
                "arg2", resource.getArg2()
        );
    }

    void grantPrivilegeToRole( String roleName, ResourcePrivilege resourcePrivilege ) throws InvalidArgumentsException
    {
        Map<String,Object> params = makePrivilegeParameters( roleName, resourcePrivilege );
        String databaseMatch = resourcePrivilege.isAllDatabases() ? "MERGE (db:DatabaseAll {name: '*'})" : "MATCH (db:Database {name: $dbName})";
        Segment segment = resourcePrivilege.getSegment();
        String qualifierPattern;
        if ( segment instanceof LabelSegment )
        {
            boolean fullSegment = segment.equals( LabelSegment.ALL );
            qualifierPattern = fullSegment ? "q:LabelQualifierAll {type: 'node', label: '*'}"
                                           : String.format( "q:LabelQualifier {type: 'node', label: '%s'}", ((LabelSegment) segment).getLabel() );
        }
        else
        {
            boolean fullSegment = segment.equals( RelTypeSegment.ALL );
            qualifierPattern = fullSegment ? "q:RelationshipQualifierAll {type: 'relationship', label: '*'}"
                                           : String.format( "q:RelationshipQualifier {type: 'relationship', label: '%s'}",
                                                   ((RelTypeSegment) segment).getRelType() );
        }

        String query = String.format(
                "MATCH (r:Role {name: $roleName}) " +
                "%s " +
                "MERGE (res:Resource {type: $resource, arg1: $arg1, arg2: $arg2}) " +
                "MERGE (%s) " +
                "MERGE (db)<-[:FOR]-(segment:Segment)-[:QUALIFIED]->(q) " +
                "MERGE (segment)<-[:SCOPE]-(p:Action {action: $action})-[:APPLIES_TO]->(res) " +
                "MERGE (r)-[:GRANTED]->(p) " +
                "RETURN id(p)",
                databaseMatch, qualifierPattern
        );
        assertPrivilegeSuccess( roleName, query, params, resourcePrivilege );
    }

    private void assertPrivilegeSuccess( String roleName, String query, Map<String,Object> params, ResourcePrivilege resourcePrivilege )
            throws InvalidArgumentsException
    {
        if ( !queryExecutor.executeQueryWithParamCheck( query, params ) )
        {
            assertRoleExists( roleName );
            if ( !resourcePrivilege.isAllDatabases() )
            {
                assertDbExists( resourcePrivilege.getDbName() );
            }
        }
    }

    Set<ResourcePrivilege> getPrivilegeForRoles( Set<String> roles )
    {
        Map<String,Set<ResourcePrivilege>> resultsPerRole = new HashMap<>();
        // check if all in cache else lookup and store in cache
        boolean lookupPrivileges = false;
        for ( String role : roles )
        {
            Set<ResourcePrivilege> privileges = privilegeCache.getIfPresent( role );
            if ( privileges == null )
            {
                lookupPrivileges = true;
            }
            else
            {
                // save cached result in output map
                resultsPerRole.put( role, privileges );
            }
        }

        if ( lookupPrivileges )
        {
            String query =
                    "MATCH (r:Role)-[:GRANTED]->(a:Action)-[:SCOPE]->(segment:Segment), " +
                            "(a)-[:APPLIES_TO]->(res) " +
                            "WHERE r.name IN $roles " +
                            "MATCH (segment)-[:FOR]->(db) " +
                            "MATCH (segment)-[:QUALIFIED]->(q) " +
                            "RETURN r.name, db.name, db, a.action, res, q";

            final ErrorPreservingQuerySubscriber subscriber = new ErrorPreservingQuerySubscriber()
            {
                private AnyValue[] fields;

                @Override
                public void onResult( int numberOfFields )
                {
                    this.fields = new AnyValue[numberOfFields];
                }

                @Override
                public void onField( int offset, AnyValue value )
                {
                    fields[offset] = value;
                }

                @Override
                public void onRecordCompleted()
                {
                    String roleName = ((TextValue) fields[0]).stringValue();
                    Set<ResourcePrivilege> rolePrivileges = resultsPerRole.computeIfAbsent( roleName, role -> new HashSet<>() );

                    AnyValue dbNameValue = fields[1];
                    NodeValue database = (NodeValue) fields[2];
                    String actionValue = ((TextValue) fields[3]).stringValue();
                    NodeValue resource = (NodeValue) fields[4];
                    NodeValue qualifier = (NodeValue) fields[5];

                    try
                    {
                        PrivilegeBuilder privilegeBuilder = PrivilegeBuilder.grant( actionValue )
                                .withinScope( qualifier )
                                .onResource( resource );
                        assert database.labels().length() == 1;
                        switch ( database.labels().stringValue( 0 ) )
                        {
                        case "Database":
                            String dbName = ((TextValue) dbNameValue).stringValue();
                            privilegeBuilder.forDatabase( dbName );
                            break;
                        case "DatabaseAll":
                            privilegeBuilder.forAllDatabases();
                            break;
                        case "DeletedDatabase":
                            //give up
                            return;
                        default:
                            throw new IllegalStateException(
                                    "Cannot have database node without either 'Database' or 'DatabaseAll' labels: " + database.labels() );
                        }

                        rolePrivileges.add( privilegeBuilder.build() );
                    }
                    catch ( InvalidArgumentsException ie )
                    {
                        throw new IllegalStateException( "Failed to authorize", ie );
                    }
                }
            };

            queryExecutor.executeQuery( query, Collections.singletonMap( "roles", roles ), subscriber );
        }

        if ( !resultsPerRole.isEmpty() )
        {
            // cache the privileges we looked up
            privilegeCache.putAll( resultsPerRole );
        }
        Set<ResourcePrivilege> combined = new HashSet<>();
        for ( Set<ResourcePrivilege> privs : resultsPerRole.values() )
        {
            combined.addAll( privs );
        }
        return combined;
    }

    void clearCacheForRole( String role )
    {
        privilegeCache.invalidate( role );
    }

    void clearCacheForRoles()
    {
        privilegeCache.invalidateAll();
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
