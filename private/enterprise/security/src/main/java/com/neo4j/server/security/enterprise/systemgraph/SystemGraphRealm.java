/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.neo4j.server.security.enterprise.auth.RealmLifecycle;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.ShiroAuthorizationInfoProvider;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.subject.PrincipalCollection;

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
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.server.security.systemgraph.SecurityGraphInitializer;

import static org.neo4j.internal.helpers.collection.Iterables.single;

/**
 * Shiro realm using a Neo4j graph to store users and roles
 */
public class SystemGraphRealm extends BasicSystemGraphRealm implements RealmLifecycle, ShiroAuthorizationInfoProvider
{
    private final boolean authorizationEnabled;
    private com.github.benmanes.caffeine.cache.Cache<String,Set<ResourcePrivilege>> privilegeCache;

    public SystemGraphRealm( SecurityGraphInitializer systemGraphInitializer, DatabaseManager<?> databaseManager,  SecureHasher secureHasher,
            AuthenticationStrategy authenticationStrategy, boolean authenticationEnabled,
            boolean authorizationEnabled )
    {
        super( systemGraphInitializer, databaseManager, secureHasher, authenticationStrategy, authenticationEnabled );
        setName( SecuritySettings.NATIVE_REALM_NAME );
        this.authorizationEnabled = authorizationEnabled;
        Caffeine<Object,Object> builder = Caffeine.newBuilder()
                .maximumSize( 10000 )
                .executor( ForkJoinPool.commonPool() )
                .expireAfterAccess( Duration.ofHours( 1 ) );
        privilegeCache = builder.build();
        setAuthorizationCachingEnabled( true );
    }

    @Override
    public void initialize()
    {
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
        catch ( NotFoundException n )
        {
            // Can occur if the user was dropped by another thread after the null check.
            // The behaviour should be the same as if the user did not exist at the start of the authorization.
            return null;
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

    public Set<ResourcePrivilege> getPrivilegesForRoles( Set<String> roles )
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
                        tx.findNodes( Label.label( "Role" ) ).stream().filter( roleNode -> rolesForUserContainsRole( roles, roleNode ) );
                roleStream.forEach( roleNode ->
                {
                    try
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

                                Node segmentNode =
                                        single( privilege.getRelationships( Direction.OUTGOING, RelationshipType.withName( "SCOPE" ) ) ).getEndNode();

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
                                    throw new IllegalStateException(
                                            "Cannot have database node without either 'Database' or 'DatabaseAll' labels: " + dbLabel );
                                }

                                rolePrivileges.add( privilegeBuilder.build() );
                            }
                            catch ( InvalidArgumentsException ie )
                            {
                                throw new IllegalStateException( "Failed to authorize", ie );
                            }
                        } );
                    }
                    catch ( NotFoundException n )
                    {
                        // Can occur if the role was dropped by another thread during the privilege lookup.
                        // The behaviour should be the same as if the user did not have the role,
                        // i.e. the role should not be added to the privilege map.
                    }
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

    private boolean rolesForUserContainsRole( Set<String> roles, Node roleNode )
    {
        try
        {
            return roles.contains( roleNode.getProperty( "name" ).toString() );
        }
        catch ( NotFoundException n )
        {
            // Can occur if the role was dropped by another thread.
            // The behaviour should be the same as if the user did not have the role.
            return false;
        }
    }

    public void clearCacheForRoles()
    {
        privilegeCache.invalidateAll();
    }
}
