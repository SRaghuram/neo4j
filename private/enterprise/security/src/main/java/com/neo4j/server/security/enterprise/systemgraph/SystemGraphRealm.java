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
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.DisabledAccountException;
import org.apache.shiro.authc.ExcessiveAttemptsException;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.apache.shiro.authc.UnknownAccountException;
import org.apache.shiro.authc.credential.CredentialsMatcher;
import org.apache.shiro.authc.pam.UnsupportedTokenException;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;

import org.neo4j.cypher.internal.security.FormatException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.ShiroAuthToken;
import org.neo4j.server.security.systemgraph.SecurityGraphInitializer;
import org.neo4j.server.security.systemgraph.SystemGraphRealmHelper;

import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.server.security.systemgraph.SystemGraphRealmHelper.IS_SUSPENDED;

/**
 * Shiro realm using a Neo4j graph to store users and roles
 */
public class SystemGraphRealm extends AuthorizingRealm implements RealmLifecycle, ShiroAuthorizationInfoProvider, CredentialsMatcher
{
    private final SecurityGraphInitializer systemGraphInitializer;
    private SystemGraphRealmHelper systemGraphRealmHelper;
    private final AuthenticationStrategy authenticationStrategy;
    private final boolean authenticationEnabled;
    private final boolean authorizationEnabled;

    private com.github.benmanes.caffeine.cache.Cache<String,Set<ResourcePrivilege>> privilegeCache;

    public SystemGraphRealm( SecurityGraphInitializer systemGraphInitializer, SystemGraphRealmHelper systemGraphRealmHelper,
            AuthenticationStrategy authenticationStrategy, boolean authenticationEnabled, boolean authorizationEnabled )
    {
        this.systemGraphInitializer = systemGraphInitializer;
        this.systemGraphRealmHelper = systemGraphRealmHelper;
        this.authenticationStrategy = authenticationStrategy;
        this.authenticationEnabled = authenticationEnabled;

        setAuthenticationCachingEnabled( true );
        setName( SecuritySettings.NATIVE_REALM_NAME );
        this.authorizationEnabled = authorizationEnabled;
        Caffeine<Object,Object> builder = Caffeine.newBuilder()
                .maximumSize( 10000 )
                .executor( ForkJoinPool.commonPool() )
                .expireAfterAccess( Duration.ofHours( 1 ) );
        privilegeCache = builder.build();
        setAuthorizationCachingEnabled( true );
        setCredentialsMatcher( this );
    }

    @Override
    public void initialize()
    {
    }

    @Override
    public void start() throws Exception
    {
        systemGraphInitializer.initializeSecurityGraph();
    }

    @Override
    public void stop()
    {
    }

    @Override
    public void shutdown()
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
    public boolean doCredentialsMatch( AuthenticationToken token, AuthenticationInfo info )
    {
        // We assume that the given info originated from this class, so we can get the user record from it
        SystemGraphAuthenticationInfo ourInfo = (SystemGraphAuthenticationInfo) info;
        User user = ourInfo.getUserRecord();

        // Get the password from the token
        byte[] password;
        try
        {
            ShiroAuthToken shiroAuthToken = (ShiroAuthToken) token;
            password = AuthToken.safeCastCredentials( AuthToken.CREDENTIALS, shiroAuthToken.getAuthTokenMap() );
        }
        catch ( InvalidAuthTokenException e )
        {
            throw new UnsupportedTokenException( e );
        }

        // Authenticate using our strategy (i.e. with rate limiting)
        AuthenticationResult result = authenticationStrategy.authenticate( user, password );

        // Map failures to exceptions
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

        // We also need to look at the user record flags
        if ( user.hasFlag( IS_SUSPENDED ) )
        {
            throw new DisabledAccountException( "User '" + user.name() + "' is suspended." );
        }

        if ( user.passwordChangeRequired() )
        {
            result = AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
        }

        // Ok, if no exception was thrown by now it was a match.
        // Modify the given AuthenticationInfo with the final result and return with success.
        ourInfo.setAuthenticationResult( result );
        return true;
    }

    @Override
    protected Object getAuthenticationCacheKey( AuthenticationToken token )
    {
        Object principal = token != null ? token.getPrincipal() : null;
        return principal != null ? principal : "";
    }

    @Override
    protected Object getAuthenticationCacheKey( PrincipalCollection principals )
    {
        Object principal = getAvailablePrincipal( principals );
        return principal == null ? "" : principal;
    }

    @Override
    public AuthenticationInfo doGetAuthenticationInfo( AuthenticationToken token )
    {
        if ( !authenticationEnabled )
        {
            return null;
        }

        ShiroAuthToken shiroAuthToken = (ShiroAuthToken) token;

        String username;
        try
        {
            username = AuthToken.safeCast( AuthToken.PRINCIPAL, shiroAuthToken.getAuthTokenMap() );
            // This is only checked here to check for InvalidAuthToken
            AuthToken.safeCastCredentials( AuthToken.CREDENTIALS, shiroAuthToken.getAuthTokenMap() );
        }
        catch ( InvalidAuthTokenException e )
        {
            throw new UnsupportedTokenException( e );
        }

        User user;
        try
        {
            user = systemGraphRealmHelper.getUser( username );
        }
        catch ( InvalidArgumentsException | FormatException e )
        {
            throw new UnknownAccountException();
        }

        // Stash the user record in the AuthenticationInfo that will be cached.
        // The credentials will then be checked when Shiro calls doCredentialsMatch()
        return new SystemGraphAuthenticationInfo( user, getName() /* Realm name */ );
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

        try ( Transaction tx = systemGraphRealmHelper.getSystemDb().beginTx() )
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

            try ( Transaction tx = systemGraphRealmHelper.getSystemDb().beginTx() )
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
