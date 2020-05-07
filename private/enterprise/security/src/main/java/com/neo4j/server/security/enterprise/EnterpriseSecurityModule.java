/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise;

import com.github.benmanes.caffeine.cache.Ticker;
import com.neo4j.dbms.ReplicatedDatabaseEventService;
import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import com.neo4j.server.security.enterprise.auth.FileRoleRepository;
import com.neo4j.server.security.enterprise.auth.InClusterAuthManager;
import com.neo4j.server.security.enterprise.auth.LdapRealm;
import com.neo4j.server.security.enterprise.auth.MultiRealmAuthManager;
import com.neo4j.server.security.enterprise.auth.RoleRepository;
import com.neo4j.server.security.enterprise.auth.SecurityProcedures;
import com.neo4j.server.security.enterprise.auth.ShiroCaffeineCache;
import com.neo4j.server.security.enterprise.auth.UserManagementProcedures;
import com.neo4j.server.security.enterprise.auth.plugin.PluginRealm;
import com.neo4j.server.security.enterprise.auth.plugin.spi.AuthPlugin;
import com.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin;
import com.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationPlugin;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphComponent;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm;
import org.apache.shiro.cache.CacheManager;
import org.apache.shiro.realm.Realm;

import java.io.File;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.commandline.admin.security.SetDefaultAdminCommand;
import org.neo4j.common.DependencySatisfier;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.systemgraph.SystemGraphRealmHelper;
import org.neo4j.service.Services;
import org.neo4j.time.Clocks;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

public class EnterpriseSecurityModule extends SecurityModule
{
    private static final String ROLE_STORE_FILENAME = "roles";
    private static final String DEFAULT_ADMIN_STORE_FILENAME = SetDefaultAdminCommand.ADMIN_INI;

    private final Config config;
    private final GlobalProcedures globalProcedures;
    private final Log log;
    private final EnterpriseSecurityGraphComponent enterpriseSecurityGraphComponent;
    private final DependencySatisfier dependencySatisfier;
    private final GlobalTransactionEventListeners transactionEventListeners;
    private EnterpriseAuthManager authManager;
    private SecurityConfig securityConfig;
    private SecureHasher secureHasher;
    private final SecurityLog securityLog;
    private AuthManager inClusterAuthManager;

    public EnterpriseSecurityModule( LogProvider logProvider,
                                     SecurityLog securityLog,
                                     Config config,
                                     GlobalProcedures procedures,
                                     DependencySatisfier dependencySatisfier,
                                     GlobalTransactionEventListeners transactionEventListeners,
                                     EnterpriseSecurityGraphComponent enterpriseSecurityGraphComponent )
    {
        this.securityLog = securityLog;
        this.config = config;
        this.globalProcedures = procedures;
        this.dependencySatisfier = dependencySatisfier;
        this.transactionEventListeners = transactionEventListeners;
        this.log = logProvider.getLog( getClass() );
        this.enterpriseSecurityGraphComponent = enterpriseSecurityGraphComponent;
    }

    @Override
    public void setup()
    {
        this.secureHasher = new SecureHasher();
        org.neo4j.collection.Dependencies platformDependencies = (org.neo4j.collection.Dependencies) dependencySatisfier;
        Supplier<GraphDatabaseService> systemSupplier = () ->
        {
            DatabaseManager<?> databaseManager = platformDependencies.resolveDependency( DatabaseManager.class );
            return databaseManager.getDatabaseContext( NAMED_SYSTEM_DATABASE_ID ).orElseThrow(
                    () -> new RuntimeException( "No database called `" + SYSTEM_DATABASE_NAME + "` was found." ) ).databaseFacade();
        };

        boolean isClustered = config.get( GraphDatabaseSettings.mode ) == GraphDatabaseSettings.Mode.CORE ||
                              config.get( GraphDatabaseSettings.mode ) == GraphDatabaseSettings.Mode.READ_REPLICA;

        authManager = newAuthManager( securityLog, systemSupplier );
        dependencySatisfier.satisfyDependency( authManager );

        AuthCacheClearingDatabaseEventListener databaseEventListener = new AuthCacheClearingDatabaseEventListener( authManager );

        if ( isClustered )
        {
            var replicatedDatabaseEventService = platformDependencies.resolveDependency( ReplicatedDatabaseEventService.class );
            replicatedDatabaseEventService.registerListener( NAMED_SYSTEM_DATABASE_ID, databaseEventListener );
        }
        else
        {
            transactionEventListeners.registerTransactionEventListener( SYSTEM_DATABASE_NAME, databaseEventListener );
        }

        // Register procedures
        globalProcedures.registerComponent( SecurityLog.class, ctx -> securityLog, false );
        globalProcedures.registerComponent( EnterpriseAuthManager.class, ctx -> authManager, false );
        globalProcedures.registerComponent( EnterpriseSecurityContext.class, ctx -> asEnterpriseEdition( ctx.securityContext() ), true );

        if ( securityConfig.nativeAuthEnabled )
        {
            if ( config.get( SecuritySettings.authentication_providers ).size() > 1 || config.get( SecuritySettings.authorization_providers ).size() > 1 )
            {
                registerProcedure( globalProcedures, log, UserManagementProcedures.class, "%s only applies to native users." );
            }
            else
            {
                registerProcedure( globalProcedures, log, UserManagementProcedures.class, null );
            }
        }
        registerProcedure( globalProcedures, log, SecurityProcedures.class, null );
    }

    @Override
    public AuthManager authManager()
    {
        return authManager;
    }

    @Override
    public AuthManager inClusterAuthManager()
    {
        return inClusterAuthManager;
    }

    private EnterpriseSecurityContext asEnterpriseEdition( SecurityContext securityContext )
    {
        if ( securityContext instanceof EnterpriseSecurityContext )
        {
            return (EnterpriseSecurityContext) securityContext;
        }
        // TODO: better handling of this possible cast failure
        throw new RuntimeException( "Expected " + EnterpriseSecurityContext.class.getName() + ", got " + securityContext.getClass().getName() );
    }

    EnterpriseAuthManager newAuthManager( SecurityLog securityLog, Supplier<GraphDatabaseService> systemSupplier )
    {
        securityConfig = getValidatedSecurityConfig( config );

        List<Realm> realms = new ArrayList<>( securityConfig.authProviders.size() + 1 );
        SecureHasher secureHasher = new SecureHasher();

        SystemGraphRealm internalRealm = createSystemGraphRealm( config, systemSupplier );
        realms.add( internalRealm );

        if ( securityConfig.hasLdapProvider )
        {
            realms.add( new LdapRealm( config, securityLog, secureHasher, securityConfig.ldapAuthentication, securityConfig.ldapAuthorization ) );
        }

        if ( !securityConfig.pluginAuthProviders.isEmpty() )
        {
            realms.addAll( createPluginRealms( config, securityLog, secureHasher, securityConfig ) );
        }

        // Select the active realms in the order they are configured
        List<Realm> orderedActiveRealms = selectOrderedActiveRealms( securityConfig.authProviders, realms );

        if ( orderedActiveRealms.isEmpty() )
        {
            throw illegalConfiguration( "No valid auth provider is active." );
        }

        // create inCluster auth manager
        var logAuthSuccess = config.get( SecuritySettings.security_log_successful_authentication );
        var defaultDatabase = config.get( GraphDatabaseSettings.default_database );

        inClusterAuthManager = new InClusterAuthManager( internalRealm, securityLog, logAuthSuccess, defaultDatabase );

        return new MultiRealmAuthManager( internalRealm, orderedActiveRealms, createCacheManager( config ),
                securityLog, config.get( SecuritySettings.security_log_successful_authentication ), config.get( GraphDatabaseSettings.default_database ) );
    }

    private SecurityConfig getValidatedSecurityConfig( Config config )
    {
        SecurityConfig securityConfig = new SecurityConfig( config );
        securityConfig.validate();
        return securityConfig;
    }

    private static List<Realm> selectOrderedActiveRealms( List<String> configuredRealms, List<Realm> availableRealms )
    {
        List<Realm> orderedActiveRealms = new ArrayList<>( configuredRealms.size() );
        for ( String configuredRealmName : configuredRealms )
        {
            for ( Realm realm : availableRealms )
            {
                if ( configuredRealmName.equals( realm.getName() ) )
                {
                    orderedActiveRealms.add( realm );
                    break;
                }
            }
        }
        return orderedActiveRealms;
    }

    public static EnterpriseSecurityGraphComponent createSecurityComponent( SecurityLog securityLog, Config config, FileSystemAbstraction fileSystem,
                                                                            LogProvider logProvider )
    {
        RoleRepository migrationRoleRepository = EnterpriseSecurityModule.getRoleRepository( config, logProvider, fileSystem );
        UserRepository defaultAdminRepository = EnterpriseSecurityModule.getDefaultAdminRepository( config, logProvider, fileSystem );

        return new EnterpriseSecurityGraphComponent( securityLog, migrationRoleRepository, defaultAdminRepository, config );
    }

    private SystemGraphRealm createSystemGraphRealm( Config config, Supplier<GraphDatabaseService> systemSupplier )
    {
        return new SystemGraphRealm(
                new SystemGraphRealmHelper( systemSupplier, secureHasher ),
                CommunitySecurityModule.createAuthenticationStrategy( config ),
                securityConfig.nativeAuthentication,
                securityConfig.nativeAuthorization,
                enterpriseSecurityGraphComponent
        );
    }

    private static CacheManager createCacheManager( Config config )
    {
        long ttl = config.get( SecuritySettings.auth_cache_ttl ).toMillis();
        boolean useTTL = config.get( SecuritySettings.auth_cache_use_ttl );
        int maxCapacity = config.get( SecuritySettings.auth_cache_max_capacity );
        return new ShiroCaffeineCache.Manager( Ticker.systemTicker(), ttl, maxCapacity, useTTL );
    }

    private static List<PluginRealm> createPluginRealms(
            Config config, SecurityLog securityLog, SecureHasher secureHasher, SecurityConfig securityConfig )
    {
        List<PluginRealm> availablePluginRealms = new ArrayList<>();
        Set<Class> excludedClasses = new HashSet<>();

        if ( securityConfig.pluginAuthentication && securityConfig.pluginAuthorization )
        {
            for ( AuthPlugin plugin : Services.loadAll( AuthPlugin.class ) )
            {
                PluginRealm pluginRealm =
                        new PluginRealm( plugin, config, securityLog, Clocks.systemClock(), secureHasher );
                availablePluginRealms.add( pluginRealm );
            }
        }

        if ( securityConfig.pluginAuthentication )
        {
            for ( AuthenticationPlugin plugin : Services.loadAll( AuthenticationPlugin.class ) )
            {
                PluginRealm pluginRealm;

                if ( securityConfig.pluginAuthorization && plugin instanceof AuthorizationPlugin )
                {
                    // This plugin implements both interfaces, create a combined plugin
                    pluginRealm = new PluginRealm( plugin, (AuthorizationPlugin) plugin, config, securityLog,
                            Clocks.systemClock(), secureHasher );

                    // We need to make sure we do not add a duplicate when the AuthorizationPlugin service gets loaded
                    // so we allow only one instance per combined plugin class
                    excludedClasses.add( plugin.getClass() );
                }
                else
                {
                    pluginRealm =
                            new PluginRealm( plugin, null, config, securityLog, Clocks.systemClock(), secureHasher );
                }
                availablePluginRealms.add( pluginRealm );
            }
        }

        if ( securityConfig.pluginAuthorization )
        {
            for ( AuthorizationPlugin plugin : Services.loadAll( AuthorizationPlugin.class ) )
            {
                if ( !excludedClasses.contains( plugin.getClass() ) )
                {
                    availablePluginRealms.add(
                            new PluginRealm( null, plugin, config, securityLog, Clocks.systemClock(), secureHasher )
                        );
                }
            }
        }

        for ( String pluginRealmName : securityConfig.pluginAuthProviders )
        {
            if ( availablePluginRealms.stream().noneMatch( r -> r.getName().equals( pluginRealmName ) ) )
            {
                throw illegalConfiguration( format( "Failed to load auth plugin '%s'.", pluginRealmName ) );
            }
        }

        List<PluginRealm> realms =
                availablePluginRealms.stream()
                        .filter( realm -> securityConfig.pluginAuthProviders.contains( realm.getName() ) )
                        .collect( Collectors.toList() );

        boolean missingAuthenticatingRealm =
                securityConfig.onlyPluginAuthentication() && realms.stream().noneMatch( PluginRealm::canAuthenticate );
        boolean missingAuthorizingRealm =
                securityConfig.onlyPluginAuthorization() && realms.stream().noneMatch( PluginRealm::canAuthorize );

        if ( missingAuthenticatingRealm || missingAuthorizingRealm )
        {
            String missingProvider =
                    ( missingAuthenticatingRealm && missingAuthorizingRealm ) ? "authentication or authorization" :
                    missingAuthenticatingRealm ? "authentication" : "authorization";

            throw illegalConfiguration( format(
                    "No plugin %s provider loaded even though required by configuration.", missingProvider ) );
        }

        return realms;
    }

    private static RoleRepository getRoleRepository( Config config, LogProvider logProvider, FileSystemAbstraction fileSystem )
    {
        return new FileRoleRepository( fileSystem, getRoleRepositoryFile( config ), logProvider );
    }

    private static UserRepository getDefaultAdminRepository( Config config, LogProvider logProvider,
            FileSystemAbstraction fileSystem )
    {
        return new FileUserRepository( fileSystem, getDefaultAdminRepositoryFile( config ), logProvider );
    }

    private static File getRoleRepositoryFile( Config config )
    {
        return new File( config.get( DatabaseManagementSystemSettings.auth_store_directory ).toFile(), ROLE_STORE_FILENAME );
    }

    private static File getDefaultAdminRepositoryFile( Config config )
    {
        return new File( config.get( DatabaseManagementSystemSettings.auth_store_directory ).toFile(),
                DEFAULT_ADMIN_STORE_FILENAME );
    }

    private static IllegalArgumentException illegalConfiguration( String message )
    {
        return new IllegalArgumentException( "Illegal configuration: " + message );
    }

    protected static class SecurityConfig
    {
        final List<String> authProviders;
        boolean hasNativeProvider;
        boolean hasLdapProvider;
        final Set<String> pluginAuthProviders;
        final List<String> pluginAuthenticationProviders;
        final List<String> pluginAuthorizationProviders;
        final boolean nativeAuthentication;
        final boolean nativeAuthorization;
        final boolean ldapAuthentication;
        final boolean ldapAuthorization;
        final boolean pluginAuthentication;
        final boolean pluginAuthorization;
        private final boolean propertyAuthorization;
        private final String propertyAuthMapping;
        final boolean nativeAuthEnabled;

        SecurityConfig( Config config )
        {
            List<String> authenticationProviders = new ArrayList<>( config.get( SecuritySettings.authentication_providers ) );
            List<String> authorizationProviders = new ArrayList<>( config.get( SecuritySettings.authorization_providers ) );

            authProviders = mergeAuthenticationAndAuthorization( authenticationProviders, authorizationProviders);

            hasNativeProvider = authenticationProviders.contains( SecuritySettings.NATIVE_REALM_NAME ) ||
                    authorizationProviders.contains( SecuritySettings.NATIVE_REALM_NAME );
            hasLdapProvider = authenticationProviders.contains( SecuritySettings.LDAP_REALM_NAME ) ||
                    authorizationProviders.contains( SecuritySettings.LDAP_REALM_NAME );

            pluginAuthenticationProviders = authenticationProviders.stream()
                    .filter( r -> r.startsWith( SecuritySettings.PLUGIN_REALM_NAME_PREFIX ) )
                    .collect( Collectors.toList() );
            pluginAuthorizationProviders = authorizationProviders.stream()
                    .filter( r -> r.startsWith( SecuritySettings.PLUGIN_REALM_NAME_PREFIX ) )
                    .collect( Collectors.toList() );

            pluginAuthProviders = new HashSet<>();
            pluginAuthProviders.addAll( pluginAuthenticationProviders );
            pluginAuthProviders.addAll( pluginAuthorizationProviders );

            nativeAuthentication = authenticationProviders.contains( SecuritySettings.NATIVE_REALM_NAME );
            nativeAuthorization = authorizationProviders.contains( SecuritySettings.NATIVE_REALM_NAME );
            nativeAuthEnabled = nativeAuthentication || nativeAuthorization;

            ldapAuthentication = authenticationProviders.contains( SecuritySettings.LDAP_REALM_NAME );
            ldapAuthorization = authorizationProviders.contains( SecuritySettings.LDAP_REALM_NAME );

            pluginAuthentication = !pluginAuthenticationProviders.isEmpty();
            pluginAuthorization = !pluginAuthorizationProviders.isEmpty();

            propertyAuthorization = config.get( SecuritySettings.property_level_authorization_enabled );
            propertyAuthMapping = config.get( SecuritySettings.property_level_authorization_permissions );
        }

        protected void validate()
        {
            if ( !nativeAuthentication && !ldapAuthentication && !pluginAuthentication )
            {
                throw illegalConfiguration( "No authentication provider found." );
            }

            if ( !nativeAuthorization && !ldapAuthorization && !pluginAuthorization )
            {
                throw illegalConfiguration( "No authorization provider found." );
            }

            if ( propertyAuthorization || propertyAuthMapping != null )
            {
                throw illegalConfiguration(
                        "Property level blacklisting through configuration setting has been replaced by privilege management on roles, e.g. " +
                        "'DENY READ {property} ON GRAPH * ELEMENTS * TO role'." );
            }
        }

        boolean onlyPluginAuthentication()
        {
            return !nativeAuthentication && !ldapAuthentication && pluginAuthentication;
        }

        boolean onlyPluginAuthorization()
        {
            return !nativeAuthorization && !ldapAuthorization && pluginAuthorization;
        }
    }

    static List<String> mergeAuthenticationAndAuthorization( List<String> authenticationProviders, List<String> authorizationProviders )
    {
        Deque<String> authorizationDeque = new ArrayDeque<>( authorizationProviders );
        List<String> authProviders = new ArrayList<>();
        for ( String authenticationProvider : authenticationProviders )
        {
            if ( authProviders.contains( authenticationProvider ) )
            {
                throw illegalConfiguration( "The relative order of authentication providers and authorization providers must match." );
            }

            if ( !authorizationDeque.contains( authenticationProvider ) )
            {
                authProviders.add( authenticationProvider );
            }
            else
            {
                // Exists in both
                while ( !authorizationDeque.isEmpty() )
                {
                    String top = authorizationDeque.pop();
                    authProviders.add( top );
                    if ( authenticationProvider.equals( top ) )
                    {
                        break;
                    }
                }
            }
        }
        authProviders.addAll( authorizationDeque );

        return authProviders;
    }
}
