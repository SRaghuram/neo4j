/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.configuration.SecuritySettings;
import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.ExcessiveAttemptsException;
import org.apache.shiro.authc.pam.ModularRealmAuthenticator;
import org.apache.shiro.authc.pam.UnsupportedTokenException;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.cache.CacheManager;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.mgt.DefaultSessionStorageEvaluator;
import org.apache.shiro.mgt.DefaultSubjectDAO;
import org.apache.shiro.mgt.SubjectDAO;
import org.apache.shiro.realm.AuthenticatingRealm;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.realm.CachingRealm;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.util.Initializable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.security.AuthProviderFailedException;
import org.neo4j.graphdb.security.AuthProviderTimeoutException;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.server.security.auth.ShiroAuthToken;

import static org.neo4j.internal.helpers.Strings.escape;
import static org.neo4j.kernel.api.security.AuthToken.invalidToken;

public class MultiRealmAuthManager extends EnterpriseAuthManager
{
    private final Collection<Realm> realms;
    private final DefaultSecurityManager securityManager;
    private final CacheManager cacheManager;
    private final SecurityLog securityLog;
    private final boolean logSuccessfulLogin;
    private final String defaultDatabase;
    private final String upgradeUsername;
    private final boolean restrictUpgrade;

    private final PrivilegeResolver privilegeResolver;

    public MultiRealmAuthManager( PrivilegeResolver privilegeResolver, Collection<Realm> realms, CacheManager cacheManager,
            SecurityLog securityLog, Config config )
    {
        this.realms = realms;
        this.cacheManager = cacheManager;

        this.upgradeUsername = config.get( GraphDatabaseInternalSettings.upgrade_username );
        this.restrictUpgrade = config.get( GraphDatabaseInternalSettings.restrict_upgrade );

        securityManager = new DefaultSecurityManager( realms );
        this.securityLog = securityLog;
        this.logSuccessfulLogin = config.get( SecuritySettings.security_log_successful_authentication );
        this.defaultDatabase = config.get( GraphDatabaseSettings.default_database );
        this.privilegeResolver = privilegeResolver;
        securityManager.setSubjectFactory( new ShiroSubjectFactory() );
        ((ModularRealmAuthenticator) securityManager.getAuthenticator())
                .setAuthenticationStrategy( new ShiroAuthenticationStrategy() );

        securityManager.setSubjectDAO( createSubjectDAO() );
    }

    private SubjectDAO createSubjectDAO()
    {
        DefaultSubjectDAO subjectDAO = new DefaultSubjectDAO();
        DefaultSessionStorageEvaluator sessionStorageEvaluator = new DefaultSessionStorageEvaluator();
        sessionStorageEvaluator.setSessionStorageEnabled( false );
        subjectDAO.setSessionStorageEvaluator( sessionStorageEvaluator );
        return subjectDAO;
    }

    @Override
    public EnterpriseLoginContext login( Map<String,Object> authToken ) throws InvalidAuthTokenException
    {
        try
        {
            EnterpriseLoginContext securityContext;

            ShiroAuthToken token = new ShiroAuthToken( authToken );
            assertValidScheme( token );

            try
            {
                securityContext = new StandardEnterpriseLoginContext(
                        this, (ShiroSubject) securityManager.login( null, token ), defaultDatabase );
                AuthenticationResult authenticationResult = securityContext.subject().getAuthenticationResult();
                if ( authenticationResult == AuthenticationResult.SUCCESS )
                {
                    if ( logSuccessfulLogin )
                    {
                        securityLog.info( securityContext.subject(), "logged in" );
                    }
                }
                else if ( authenticationResult == AuthenticationResult.PASSWORD_CHANGE_REQUIRED )
                {
                    securityLog.info( securityContext.subject(), "logged in (password change required)" );
                }
                else
                {
                    String errorMessage = ((StandardEnterpriseLoginContext.NeoShiroSubject) securityContext.subject())
                            .getAuthenticationFailureMessage();
                    securityLog.error( "[%s]: failed to log in: %s", escape( token.getPrincipal().toString() ), errorMessage );
                }
                // No need to keep full Shiro authentication info around on the subject
                ((StandardEnterpriseLoginContext.NeoShiroSubject) securityContext.subject()).clearAuthenticationInfo();
            }
            catch ( UnsupportedTokenException e )
            {
                securityLog.error( "Unknown user failed to log in: %s", e.getMessage() );
                Throwable cause = e.getCause();
                if ( cause instanceof InvalidAuthTokenException )
                {
                    throw new InvalidAuthTokenException( cause.getMessage() + ": " + token );
                }
                throw invalidToken( ": " + token );
            }
            catch ( ExcessiveAttemptsException e )
            {
                // NOTE: We only get this with single (internal) realm authentication
                securityContext = new StandardEnterpriseLoginContext( this, new ShiroSubject( securityManager, AuthenticationResult.TOO_MANY_ATTEMPTS ),
                        defaultDatabase );
                securityLog.error( "[%s]: failed to log in: too many failed attempts",
                        escape( token.getPrincipal().toString() ) );
            }
            catch ( AuthenticationException e )
            {
                if ( e.getCause() != null && e.getCause() instanceof AuthProviderTimeoutException )
                {
                    Throwable cause = e.getCause().getCause();
                    securityLog.error( "[%s]: failed to log in: auth server timeout%s",
                            escape( token.getPrincipal().toString() ),
                            cause != null && cause.getMessage() != null ? " (" + cause.getMessage() + ")" : "" );
                    throw new AuthProviderTimeoutException( e.getCause().getMessage(), e.getCause() );
                }
                else if ( e.getCause() != null && e.getCause() instanceof AuthProviderFailedException )
                {
                    Throwable cause = e.getCause().getCause();
                    securityLog.error( "[%s]: failed to log in: auth server connection refused%s",
                            escape( token.getPrincipal().toString() ),
                            cause != null && cause.getMessage() != null ? " (" + cause.getMessage() + ")" : "" );
                    throw new AuthProviderFailedException( e.getCause().getMessage(), e.getCause() );
                }
                securityContext = new StandardEnterpriseLoginContext( this,
                                                                      new ShiroSubject( securityManager, AuthenticationResult.FAILURE ), defaultDatabase );
                Throwable cause = e.getCause();
                Throwable causeCause = e.getCause() != null ? e.getCause().getCause() : null;
                String errorMessage = String.format( "invalid principal or credentials%s%s",
                        cause != null && cause.getMessage() != null ? " (" + cause.getMessage() + ")" : "",
                        causeCause != null && causeCause.getMessage() != null ? " (" + causeCause.getMessage() + ")" : "" );
                securityLog.error( "[%s]: failed to log in: %s", escape( token.getPrincipal().toString() ), errorMessage );
            }

            return securityContext;
        }
        finally
        {
            AuthToken.clearCredentials( authToken );
        }
    }

    @Override
    public void log( String message, SecurityContext securityContext )
    {
        securityLog.info( securityContext.subject(), message );
    }

    private void assertValidScheme( ShiroAuthToken token ) throws InvalidAuthTokenException
    {
        String scheme = token.getSchemeSilently();
        if ( scheme == null )
        {
            throw invalidToken( "missing key `scheme`: " + token );
        }
        else if ( scheme.equals( "none" ) )
        {
            throw invalidToken( "scheme='none' only allowed when auth is disabled: " + token );
        }
    }

    @Override
    public void init() throws Exception
    {
        for ( Realm realm : realms )
        {
            if ( !(realm instanceof SystemGraphRealm) )
            {
                if ( realm instanceof Initializable )
                {
                    ((Initializable) realm).init();
                }
                if ( realm instanceof CachingRealm )
                {
                    ((CachingRealm) realm).setCacheManager( cacheManager );
                }
                if ( realm instanceof RealmLifecycle )
                {
                    ((RealmLifecycle) realm).initialize();
                }
            }
        }
        privilegeResolver.initAndSetCacheManager( cacheManager );
    }

    @Override
    public void start() throws Exception
    {
        for ( Realm realm : realms )
        {
            if ( !(realm instanceof SystemGraphRealm) )
            {
                if ( realm instanceof RealmLifecycle )
                {
                    ((RealmLifecycle) realm).start();
                }
            }
        }
        privilegeResolver.start();
    }

    @Override
    public void stop() throws Exception
    {
        for ( Realm realm : realms )
        {
            if ( realm instanceof RealmLifecycle )
            {
                ((RealmLifecycle) realm).stop();
            }
        }
    }

    @Override
    public void shutdown() throws Exception
    {
        for ( Realm realm : realms )
        {
            if ( realm instanceof CachingRealm )
            {
                ((CachingRealm) realm).setCacheManager( null );
            }
            if ( realm instanceof RealmLifecycle )
            {
                ((RealmLifecycle) realm).shutdown();
            }
        }
    }

    @Override
    public void clearAuthCache()
    {
        for ( Realm realm : realms )
        {
            if ( realm instanceof AuthenticatingRealm )
            {
                Cache<Object,AuthenticationInfo> cache = ((AuthenticatingRealm) realm).getAuthenticationCache();
                if ( cache != null )
                {
                    cache.clear();
                }
            }
            if ( realm instanceof AuthorizingRealm )
            {
                Cache<Object,AuthorizationInfo> cache = ((AuthorizingRealm) realm).getAuthorizationCache();
                if ( cache != null )
                {
                    cache.clear();
                }
            }
        }
        privilegeResolver.clearCacheForRoles();
    }

    Collection<AuthorizationInfo> getAuthorizationInfo( PrincipalCollection principalCollection )
    {
        List<AuthorizationInfo> infoList = new ArrayList<>( 1 );
        for ( Realm realm : realms )
        {
            if ( realm instanceof ShiroAuthorizationInfoProvider )
            {
                AuthorizationInfo info = ((ShiroAuthorizationInfoProvider) realm)
                        .getAuthorizationInfoSnapshot( principalCollection );
                if ( info != null )
                {
                    infoList.add( info );
                }
            }
        }
        return infoList;
    }

    boolean shouldGetPublicRole( Object principal )
    {
        if ( restrictUpgrade )
        {
            return !principal.equals( upgradeUsername );
        }
        return true;
    }

    Set<ResourcePrivilege> getPermissions( Set<String> roles, String username )
    {
        return privilegeResolver.getPrivileges( roles, username );
    }
}
