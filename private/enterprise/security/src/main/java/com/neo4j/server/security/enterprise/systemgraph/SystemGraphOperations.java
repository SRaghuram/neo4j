/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;

import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.server.security.systemgraph.BasicSystemGraphOperations;

import static org.neo4j.internal.helpers.collection.Iterables.single;

public class SystemGraphOperations extends BasicSystemGraphOperations
{
    private com.github.benmanes.caffeine.cache.Cache<String,Set<ResourcePrivilege>> privilegeCache;

    public SystemGraphOperations( DatabaseManager<?> databaseManager, SecureHasher secureHasher )
    {
        super( databaseManager, secureHasher );
        Caffeine<Object,Object> builder = Caffeine.newBuilder()
                .maximumSize( 10000 )
                .executor( ForkJoinPool.commonPool() )
                .expireAfterAccess( Duration.ofHours( 1 ) );
        privilegeCache = builder.build();
    }

    AuthorizationInfo doGetAuthorizationInfo( String username )
    {
        boolean existingUser = false;
        boolean passwordChangeRequired = false;
        boolean suspended = false;
        Set<String> roleNames = new TreeSet<>();

        try ( Transaction tx = getSystemDb().beginTx() )
        {
            Node userNode = tx.findNode( Label.label( "User" ), "name", username );

            if ( userNode != null )
            {
                existingUser = true;
                passwordChangeRequired = (boolean) userNode.getProperty( "passwordChangeRequired" );
                suspended = (boolean) userNode.getProperty( "suspended" );

                final Iterable<Relationship> rels = userNode.getRelationships( Direction.OUTGOING, RelationshipType.withName( "HAS_ROLE" ) );
                rels.forEach( rel -> roleNames.add( (String) rel.getEndNode().getProperty( "name" ) ) );
            }
            tx.commit();
        }

        if ( !existingUser )
        {
            return null;
        }

        if ( passwordChangeRequired || suspended )
        {
            return new SimpleAuthorizationInfo();
        }

        return new SimpleAuthorizationInfo( roleNames );
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

            try ( Transaction tx = getSystemDb().beginTx() )
            {
                final Stream<Node> roleStream =
                        tx.findNodes( Label.label( "Role" ) ).stream().filter( roleNode -> roles.contains( roleNode.getProperty( "name" ).toString() ) );
                roleStream.forEach( roleNode ->
                {
                    String roleName = (String) roleNode.getProperty( "name" );
                    Set<ResourcePrivilege> rolePrivileges = resultsPerRole.computeIfAbsent( roleName, role -> new HashSet<>() );

                    roleNode.getRelationships( Direction.OUTGOING ).forEach( relToPriv ->
                    {
                        try
                        {
                            final Node privilege = relToPriv.getEndNode();
                            String grantOrDeny = relToPriv.getType().name();
                            String action = (String) privilege.getProperty( "action" );

                            Node resourceNode =
                                    single( privilege.getRelationships( Direction.OUTGOING, RelationshipType.withName( "APPLIES_TO" ) ) ).getEndNode();
                            Node segmentNode = single( privilege.getRelationships( Direction.OUTGOING, RelationshipType.withName( "SCOPE" ) ) ).getEndNode();
                            Node dbNode = single( segmentNode.getRelationships( Direction.OUTGOING, RelationshipType.withName( "FOR" ) ) ).getEndNode();
                            String dbName = (String) dbNode.getProperty( "name" );
                            Node qualifierNode =
                                    single( segmentNode.getRelationships( Direction.OUTGOING, RelationshipType.withName( "QUALIFIED" ) ) ).getEndNode();

                            ResourcePrivilege.GrantOrDeny privilegeType = ResourcePrivilege.GrantOrDeny.fromRelType( grantOrDeny );
                            PrivilegeBuilder privilegeBuilder = new PrivilegeBuilder( privilegeType, action );

                            privilegeBuilder.withinScope( qualifierNode ).onResource( resourceNode );

                            String dbLabel = single( dbNode.getLabels() ).name();
                            switch ( dbLabel )
                            {
                            case "Database":
                                privilegeBuilder.forDatabase( dbName );
                                break;
                            case "DatabaseAll":
                                privilegeBuilder.forAllDatabases();
                                break;
                            case "DeletedDatabase":
                                //give up
                                return;
                            default:
                                throw new IllegalStateException( "Cannot have database node without either 'Database' or 'DatabaseAll' labels: " + dbLabel );
                            }

                            rolePrivileges.add( privilegeBuilder.build() );
                        }
                        catch ( InvalidArgumentsException ie )
                        {
                            throw new IllegalStateException( "Failed to authorize", ie );
                        }
                    } );
                } );
                tx.commit();
            }
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

    void clearCacheForRoles()
    {
        privilegeCache.invalidateAll();
    }
}
