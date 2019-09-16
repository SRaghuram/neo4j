/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise;

import com.github.benmanes.caffeine.cache.Ticker;
import com.neo4j.dbms.ReplicatedDatabaseEventService;
import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import com.neo4j.server.security.enterprise.auth.EnterpriseAuthAndUserManager;
import com.neo4j.server.security.enterprise.auth.EnterpriseUserManager;
import com.neo4j.server.security.enterprise.auth.FileRoleRepository;
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
import com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphInitializer;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphImportOptions;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphOperations;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm;
import org.apache.shiro.cache.CacheManager;
import org.apache.shiro.realm.Realm;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.commandline.admin.security.SetDefaultAdminCommand;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.SystemGraphInitializer;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.module.DatabaseInitializer;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.KernelTransactionFactory;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.SecureHasher;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.systemgraph.ContextSwitchingSystemGraphQueryExecutor;
import org.neo4j.server.security.systemgraph.ErrorPreservingQuerySubscriber;
import org.neo4j.server.security.systemgraph.QueryExecutor;
import org.neo4j.server.security.systemgraph.SecurityGraphInitializer;
import org.neo4j.service.Services;
import org.neo4j.time.Clocks;
import org.neo4j.values.virtual.MapValue;

import static com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings.ENTERPRISE_SECURITY_MODULE_ID;
import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;
import static org.neo4j.kernel.impl.util.ValueUtils.asParameterMapValue;

@ServiceProvider
public class EnterpriseSecurityModule extends SecurityModule
{
    private static final String ROLE_STORE_FILENAME = "roles";
    public static final String IMPORT_AUTH_COMMAND_NAME = "import-auth";
    private static final String USER_IMPORT_FILENAME = ".users.import";
    private static final String ROLE_IMPORT_FILENAME = ".roles.import";
    private static final String DEFAULT_ADMIN_STORE_FILENAME = SetDefaultAdminCommand.ADMIN_INI;

    private DatabaseManager<?> databaseManager;
    private boolean isClustered;
    private Config config;
    private LogProvider logProvider;
    private FileSystemAbstraction fileSystem;
    private SystemGraphInitializer systemGraphInitializer;
    private EnterpriseAuthAndUserManager authManager;
    private SecurityConfig securityConfig;
    private ThreadToStatementContextBridge threadToStatementContextBridge;

    @Override
    public String getName()
    {
        return ENTERPRISE_SECURITY_MODULE_ID;
    }

    @Override
    public void setup( Dependencies dependencies ) throws KernelException, IOException
    {
        org.neo4j.collection.Dependencies platformDependencies = (org.neo4j.collection.Dependencies) dependencies.dependencySatisfier();
        this.databaseManager = platformDependencies.resolveDependency( DatabaseManager.class );
        this.threadToStatementContextBridge = platformDependencies.resolveDependency( ThreadToStatementContextBridge.class );
        this.systemGraphInitializer = platformDependencies.resolveDependency( SystemGraphInitializer.class );

        this.config = dependencies.config();
        this.logProvider = dependencies.logService().getUserLogProvider();
        this.fileSystem = dependencies.fileSystem();

        isClustered = config.get( EnterpriseEditionSettings.mode ) == EnterpriseEditionSettings.Mode.CORE ||
                      config.get( EnterpriseEditionSettings.mode ) == EnterpriseEditionSettings.Mode.READ_REPLICA;

        GlobalProcedures globalProcedures = dependencies.procedures();
        JobScheduler jobScheduler = dependencies.scheduler();

        SecurityLog securityLog = SecurityLog.create( config, fileSystem, jobScheduler );
        life.add( securityLog );

        authManager = newAuthManager( config, logProvider, securityLog, fileSystem );
        life.add( dependencies.dependencySatisfier().satisfyDependency( authManager ) );

        AuthCacheClearingDatabaseEventListener databaseEventListener = new AuthCacheClearingDatabaseEventListener( authManager );

        if ( isClustered )
        {
            var replicatedDatabaseEventService = platformDependencies.resolveDependency( ReplicatedDatabaseEventService.class );
            replicatedDatabaseEventService.registerListener( SYSTEM_DATABASE_ID, databaseEventListener );
        }
        else
        {
            var standaloneTxEventListeners = dependencies.transactionEventListeners();
            standaloneTxEventListeners.registerTransactionEventListener( SYSTEM_DATABASE_NAME, databaseEventListener );
        }

        // Register procedures
        globalProcedures.registerComponent( SecurityLog.class, ctx -> securityLog, false );
        globalProcedures.registerComponent( EnterpriseAuthManager.class, ctx -> authManager, false );
        globalProcedures.registerComponent( EnterpriseSecurityContext.class, ctx -> asEnterpriseEdition( ctx.securityContext() ), true );

        if ( securityConfig.nativeAuthEnabled )
        {
            // TODO shouldn't this be registered always now with assignable privileges?
            globalProcedures.registerComponent( EnterpriseUserManager.class,
                    ctx -> authManager.getUserManager( ctx.securityContext().subject(), ctx.securityContext().isAdmin() ), true );
            if ( config.get( SecuritySettings.authentication_providers ).size() > 1 || config.get( SecuritySettings.authorization_providers ).size() > 1 )
            {
                globalProcedures.registerProcedure( UserManagementProcedures.class, true, "%s only applies to native users." );
            }
            else
            {
                globalProcedures.registerProcedure( UserManagementProcedures.class, true );
            }
        }
        else
        {
            globalProcedures.registerComponent( EnterpriseUserManager.class, ctx -> EnterpriseUserManager.NOOP, true );
        }

        globalProcedures.registerProcedure( SecurityProcedures.class, true );
    }

    @Override
    public AuthManager authManager()
    {
        return authManager;
    }

    @Override
    public UserManagerSupplier userManagerSupplier()
    {
        return authManager;
    }

    public Optional<DatabaseInitializer> getDatabaseInitializer()
    {
        if ( !securityConfig.hasNativeProvider )
        {
            return Optional.empty();
        }

        return Optional.of( database ->
        {

            QueryExecutor queryExecutor = new SecurityQueryExecutor( database );
            SecureHasher secureHasher = new SecureHasher();
            SystemGraphOperations systemGraphOperations = new SystemGraphOperations( queryExecutor, secureHasher );
            SystemGraphImportOptions importOptions = configureImportOptions( config, logProvider, fileSystem );
            Log log = logProvider.getLog( getClass() );
            EnterpriseSecurityGraphInitializer initializer =
                    new EnterpriseSecurityGraphInitializer( systemGraphInitializer, queryExecutor, log, systemGraphOperations, importOptions,
                            secureHasher );
            try
            {
                initializer.initializeSecurityGraph( database );
            }
            catch ( Throwable e )
            {
                throw new RuntimeException( e );
            }
        } );
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

    EnterpriseAuthAndUserManager newAuthManager( Config config, LogProvider logProvider, SecurityLog securityLog, FileSystemAbstraction fileSystem )
    {
        securityConfig = getValidatedSecurityConfig( config );

        List<Realm> realms = new ArrayList<>( securityConfig.authProviders.size() + 1 );
        SecureHasher secureHasher = new SecureHasher();

        EnterpriseUserManager internalRealm = createSystemGraphRealm( config, logProvider, fileSystem, securityLog );
        realms.add( (Realm) internalRealm );

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

        return new MultiRealmAuthManager( internalRealm, orderedActiveRealms, createCacheManager( config ),
                securityLog, config.get( SecuritySettings.security_log_successful_authentication ),
                securityConfig.propertyAuthorization, securityConfig.propertyBlacklist );
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

    private SystemGraphRealm createSystemGraphRealm( Config config, LogProvider logProvider, FileSystemAbstraction fileSystem, SecurityLog securityLog )
    {
        ContextSwitchingSystemGraphQueryExecutor queryExecutor =
                new ContextSwitchingSystemGraphQueryExecutor( databaseManager, threadToStatementContextBridge );

        SecureHasher secureHasher = new SecureHasher();
        SystemGraphOperations systemGraphOperations = new SystemGraphOperations( queryExecutor, secureHasher );

        SecurityGraphInitializer securityGraphInitializer =
                isClustered ? SecurityGraphInitializer.NO_OP : new EnterpriseSecurityGraphInitializer( systemGraphInitializer, queryExecutor,
                        securityLog, systemGraphOperations, configureImportOptions( config, logProvider, fileSystem ), secureHasher );

        return new SystemGraphRealm(
                systemGraphOperations,
                securityGraphInitializer,
                secureHasher,
                new BasicPasswordPolicy(),
                CommunitySecurityModule.createAuthenticationStrategy( config ),
                securityConfig.nativeAuthentication,
                securityConfig.nativeAuthorization
        );
    }

    private static SystemGraphImportOptions configureImportOptions( Config config, LogProvider logProvider, FileSystemAbstraction fileSystem )
    {
        File parentFile = CommunitySecurityModule.getUserRepositoryFile( config ).getParentFile();
        File userImportFile = new File( parentFile, USER_IMPORT_FILENAME );
        File roleImportFile = new File( parentFile, ROLE_IMPORT_FILENAME );

        boolean shouldPerformImport = fileSystem.fileExists( userImportFile ) || fileSystem.fileExists( roleImportFile );
        boolean mayPerformMigration = !shouldPerformImport;
        boolean shouldPurgeImportRepositoriesAfterSuccessfulImport = shouldPerformImport;
        boolean shouldResetSystemGraphAuthBeforeImport = false;

        Supplier<UserRepository> importUserRepositorySupplier = () -> new FileUserRepository( fileSystem, userImportFile, logProvider );
        Supplier<RoleRepository> importRoleRepositorySupplier = () -> new FileRoleRepository( fileSystem, roleImportFile, logProvider );
        Supplier<UserRepository> migrationUserRepositorySupplier = () -> CommunitySecurityModule.getUserRepository( config, logProvider, fileSystem );
        Supplier<RoleRepository> migrationRoleRepositorySupplier = () -> EnterpriseSecurityModule.getRoleRepository( config, logProvider, fileSystem );
        Supplier<UserRepository> initialUserRepositorySupplier = () -> CommunitySecurityModule.getInitialUserRepository( config, logProvider, fileSystem );
        Supplier<UserRepository> defaultAdminRepositorySupplier = () -> getDefaultAdminRepository( config, logProvider, fileSystem );

        return new SystemGraphImportOptions(
                shouldPerformImport,
                mayPerformMigration,
                shouldPurgeImportRepositoriesAfterSuccessfulImport,
                shouldResetSystemGraphAuthBeforeImport,
                importUserRepositorySupplier,
                importRoleRepositorySupplier,
                migrationUserRepositorySupplier,
                migrationRoleRepositorySupplier,
                initialUserRepositorySupplier,
                defaultAdminRepositorySupplier
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
        final boolean propertyAuthorization;
        private final String propertyAuthMapping;
        final Map<String,List<String>> propertyBlacklist = new HashMap<>();
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

            if ( propertyAuthorization && !parsePropertyPermissions() )
            {
                throw illegalConfiguration(
                        "Property level authorization is enabled but there is a error in the permissions mapping." );
            }
        }

        boolean parsePropertyPermissions()
        {
            if ( propertyAuthMapping != null && !propertyAuthMapping.isEmpty() )
            {
                String rolePattern = "\\s*[a-zA-Z0-9_]+\\s*";
                String propertyPattern = "\\s*[a-zA-Z0-9_]+\\s*";
                String roleToPerm = rolePattern + "=" + propertyPattern + "(," + propertyPattern + ")*";
                String multiLine = roleToPerm + "(;" + roleToPerm + ")*";

                boolean valid = propertyAuthMapping.matches( multiLine );
                if ( !valid )
                {
                    return false;
                }

                for ( String rolesAndPermissions : propertyAuthMapping.split( ";" ) )
                {
                    if ( !rolesAndPermissions.isEmpty() )
                    {
                        String[] split = rolesAndPermissions.split( "=" );
                        String role = split[0].trim();
                        String permissions = split[1];
                        List<String> permissionsList = new ArrayList<>();
                        for ( String perm : permissions.split( "," ) )
                        {
                            if ( !perm.isEmpty() )
                            {
                                permissionsList.add( perm.trim() );
                            }
                        }
                        propertyBlacklist.put( role, permissionsList );
                    }
                }
            }
            return true;
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

    private static class SecurityQueryExecutor implements QueryExecutor
    {
        private final TransactionalContextFactory contextFactory;
        private final GraphDatabaseAPI api;
        private final QueryExecutionEngine engine;

        private SecurityQueryExecutor( GraphDatabaseService database )
        {
            this.api = (GraphDatabaseAPI) database;
            var resolver = api.getDependencyResolver();
            this.engine = resolver.resolveDependency( QueryExecutionEngine.class );
            var bridge = resolver.resolveDependency( ThreadToStatementContextBridge.class );
            var transactionFactory = resolver.resolveDependency( KernelTransactionFactory.class );
            var embeddedProxySPI = resolver.resolveDependency( EmbeddedProxySPI.class );
            this.contextFactory = Neo4jTransactionalContextFactory.create( embeddedProxySPI,
                    () -> resolver.resolveDependency( GraphDatabaseQueryService.class ), transactionFactory, bridge );
        }

        @Override
        public void executeQuery( String query, Map<String,Object> params, ErrorPreservingQuerySubscriber subscriber )
        {
            try ( InternalTransaction tx = api.beginTransaction( KernelTransaction.Type.explicit, AUTH_DISABLED ) )
            {
                MapValue parameters = asParameterMapValue( params );
                TransactionalContext context = contextFactory.newContext( tx, query, parameters );
                QueryExecution execution =
                        engine.executeQuery( query,
                                parameters,
                                context,
                                false,
                                subscriber );
                execution.consumeAll();
                tx.commit();
            }
            catch ( Exception e )
            {
                throw new IllegalStateException( "Failed to request data", e );
            }
        }

        @Override
        public Transaction beginTx()
        {
            return api.beginTx();
        }
    }
}
