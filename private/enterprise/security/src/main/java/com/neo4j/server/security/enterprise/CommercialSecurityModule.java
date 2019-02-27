/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise;

import com.github.benmanes.caffeine.cache.Ticker;
import com.neo4j.kernel.enterprise.api.security.CommercialAuthManager;
import com.neo4j.kernel.enterprise.api.security.CommercialSecurityContext;
import com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings;
import com.neo4j.server.security.enterprise.auth.CommercialAuthAndUserManager;
import com.neo4j.server.security.enterprise.auth.EnterpriseUserManager;
import com.neo4j.server.security.enterprise.auth.FileRoleRepository;
import com.neo4j.server.security.enterprise.auth.InternalFlatFileRealm;
import com.neo4j.server.security.enterprise.auth.LdapRealm;
import com.neo4j.server.security.enterprise.auth.MultiRealmAuthManager;
import com.neo4j.server.security.enterprise.auth.RoleRepository;
import com.neo4j.server.security.enterprise.auth.SecureHasher;
import com.neo4j.server.security.enterprise.auth.SecurityProcedures;
import com.neo4j.server.security.enterprise.auth.ShiroCaffeineCache;
import com.neo4j.server.security.enterprise.auth.UserManagementProcedures;
import com.neo4j.server.security.enterprise.auth.plugin.PluginRealm;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.ContextSwitchingSystemGraphQueryExecutor;
import com.neo4j.server.security.enterprise.systemgraph.QueryExecutor;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphImportOptions;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphInitializer;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphOperations;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm;
import org.apache.shiro.cache.CacheManager;
import org.apache.shiro.realm.Realm;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.commandline.admin.security.SetDefaultAdminCommand;
import org.neo4j.common.Service;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.javacompat.QueryResultProvider;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.module.DatabaseInitializer;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationPlugin;
import org.neo4j.time.Clocks;

import static com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings.COMMERCIAL_SECURITY_MODULE_ID;
import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class CommercialSecurityModule extends SecurityModule
{
    public static final String ROLE_STORE_FILENAME = "roles";
    public static final String IMPORT_AUTH_COMMAND_NAME = "import-auth";
    public static final String USER_IMPORT_FILENAME = ".users.import";
    public static final String ROLE_IMPORT_FILENAME = ".roles.import";
    private static final String DEFAULT_ADMIN_STORE_FILENAME = SetDefaultAdminCommand.ADMIN_INI;

    private DatabaseManager databaseManager;
    private boolean initSystemGraphOnStart;
    private Config config;
    private LogProvider logProvider;
    private FileSystemAbstraction fileSystem;
    private AccessCapability accessCapability;
    private CommercialAuthAndUserManager authManager;
    private SecurityConfig securityConfig;

    public CommercialSecurityModule()
    {
        super( COMMERCIAL_SECURITY_MODULE_ID );
    }

    public CommercialSecurityModule( String securityModuleId )
    {
        super( securityModuleId );
    }

    @Override
    public void setup( Dependencies dependencies ) throws KernelException
    {
        // This will be need as an input to the SystemGraphRealm later to be able to handle transactions
        org.neo4j.kernel.impl.util.Dependencies platformDependencies = (org.neo4j.kernel.impl.util.Dependencies) dependencies.dependencySatisfier();
        this.databaseManager = platformDependencies.resolveDependency( DatabaseManager.class );

        this.config = dependencies.config();
        this.logProvider = dependencies.logService().getUserLogProvider();
        this.fileSystem = dependencies.fileSystem();
        this.accessCapability = dependencies.accessCapability();

        if ( config.get( CommercialEditionSettings.mode ) == CommercialEditionSettings.Mode.CORE ||
                config.get( CommercialEditionSettings.mode ) == CommercialEditionSettings.Mode.READ_REPLICA )
        {
            initSystemGraphOnStart = false;
        }
        else
        {
            initSystemGraphOnStart = true;
        }

        GlobalProcedures globalProcedures = dependencies.procedures();
        JobScheduler jobScheduler = dependencies.scheduler();

        SecurityLog securityLog = SecurityLog.create(
                config,
                dependencies.logService().getInternalLog( GraphDatabaseFacade.class ),
                fileSystem,
                jobScheduler
            );
        life.add( securityLog );

        authManager = newAuthManager( config, logProvider, securityLog, fileSystem, jobScheduler, accessCapability );
        life.add( dependencies.dependencySatisfier().satisfyDependency( authManager ) );

        // Register procedures
        globalProcedures.registerComponent( SecurityLog.class, ctx -> securityLog, false );
        globalProcedures.registerComponent( CommercialAuthManager.class, ctx -> authManager, false );
        globalProcedures.registerComponent( CommercialSecurityContext.class,
                ctx -> asCommercialEdition( ctx.securityContext() ), true );

        if ( securityConfig.nativeAuthEnabled )
        {
            globalProcedures.registerComponent( EnterpriseUserManager.class,
                    ctx -> authManager.getUserManager( ctx.securityContext().subject(), ctx.securityContext().isAdmin() ), true );
            if ( config.get( SecuritySettings.auth_providers ).size() > 1 )
            {
                globalProcedures.registerProcedure( UserManagementProcedures.class, true, "%s only applies to native users."  );
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
        if ( !((CommercialSecurityConfig) securityConfig).hasSystemGraphProvider )
        {
            return Optional.empty();
        }

        return Optional.of( database ->
        {
            QueryExecutor queryExecutor = new QueryExecutor()
            {
                @Override
                public void executeQuery( String query, Map<String,Object> params, QueryResult.QueryResultVisitor resultVisitor )
                {
                    try ( Transaction tx = database.beginTx() )
                    {
                        Result result = database.execute( query, params );
                        QueryResult queryResult = ((QueryResultProvider) result).queryResult();
                        queryResult.accept( resultVisitor );
                        tx.success();
                    }
                }

                @Override
                public Transaction beginTx()
                {
                    return database.beginTx();
                }
            };

            SecureHasher secureHasher = new SecureHasher();
            SystemGraphOperations systemGraphOperations = new SystemGraphOperations( queryExecutor, secureHasher );
            SystemGraphImportOptions importOptions = configureImportOptions( config, logProvider, fileSystem, accessCapability );
            Log log = logProvider.getLog( getClass() );
            SystemGraphInitializer initializer = new SystemGraphInitializer( queryExecutor, systemGraphOperations, importOptions, secureHasher, log );

            try
            {
                initializer.initializeSystemGraph();
            }
            catch ( Throwable e )
            {
                throw new RuntimeException( e );
            }
        } );
    }

    private CommercialSecurityContext asCommercialEdition( SecurityContext securityContext )
    {
        if ( securityContext instanceof CommercialSecurityContext )
        {
            return (CommercialSecurityContext) securityContext;
        }
        // TODO: better handling of this possible cast failure
        throw new RuntimeException( "Expected " + CommercialSecurityContext.class.getName() + ", got " + securityContext.getClass().getName() );
    }

    CommercialAuthAndUserManager newAuthManager( Config config, LogProvider logProvider, SecurityLog securityLog, FileSystemAbstraction fileSystem,
            JobScheduler jobScheduler, AccessCapability accessCapability )
    {
        securityConfig = getValidatedSecurityConfig( config );

        List<Realm> realms = new ArrayList<>( securityConfig.authProviders.size() + 1 );
        SecureHasher secureHasher = new SecureHasher();

        EnterpriseUserManager internalRealm = createInternalRealm( config, logProvider, fileSystem, jobScheduler, securityLog, accessCapability );
        if ( internalRealm != null )
        {
            realms.add( (Realm) internalRealm );
        }

        if ( securityConfig.hasLdapProvider )
        {
            realms.add( new LdapRealm( config, securityLog, secureHasher ) );
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
        SecurityConfig securityConfig = new CommercialSecurityConfig( config );
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

    private EnterpriseUserManager createInternalRealm( Config config, LogProvider logProvider, FileSystemAbstraction fileSystem, JobScheduler jobScheduler,
            SecurityLog securityLog, AccessCapability accessCapability )
    {
        EnterpriseUserManager internalRealm = null;
        if ( securityConfig.hasNativeProvider )
        {
            internalRealm = createInternalFlatFileRealm( config, logProvider, fileSystem, jobScheduler );
        }
        else if ( ( (CommercialSecurityConfig) securityConfig ).hasSystemGraphProvider )
        {
            internalRealm = createSystemGraphRealm( config, logProvider, fileSystem, securityLog, accessCapability );
        }
        return internalRealm;
    }

    private static InternalFlatFileRealm createInternalFlatFileRealm( Config config, LogProvider logProvider, FileSystemAbstraction fileSystem,
            JobScheduler jobScheduler )
    {
        return new InternalFlatFileRealm(
                CommunitySecurityModule.getUserRepository( config, logProvider, fileSystem ),
                getRoleRepository( config, logProvider, fileSystem ),
                new BasicPasswordPolicy(),
                createAuthenticationStrategy( config ),
                config.get( SecuritySettings.native_authentication_enabled ),
                config.get( SecuritySettings.native_authorization_enabled ),
                jobScheduler,
                CommunitySecurityModule.getInitialUserRepository( config, logProvider, fileSystem ),
                getDefaultAdminRepository( config, logProvider, fileSystem )
            );
    }

    private static AuthenticationStrategy createAuthenticationStrategy( Config config )
    {
        return new RateLimitedAuthenticationStrategy( Clocks.systemClock(), config );
    }

    private SystemGraphRealm createSystemGraphRealm( Config config, LogProvider logProvider, FileSystemAbstraction fileSystem, SecurityLog securityLog,
            AccessCapability accessCapability )
    {
        ContextSwitchingSystemGraphQueryExecutor queryExecutor = new ContextSwitchingSystemGraphQueryExecutor( databaseManager,
                config.get( GraphDatabaseSettings.active_database ) );

        SecureHasher secureHasher = new SecureHasher();
        SystemGraphOperations systemGraphOperations = new SystemGraphOperations( queryExecutor, secureHasher );

        SystemGraphInitializer systemGraphInitializer = initSystemGraphOnStart ? new SystemGraphInitializer( queryExecutor, systemGraphOperations,
                configureImportOptions( config, logProvider, fileSystem, accessCapability ), secureHasher, securityLog ) : null;

        return new SystemGraphRealm(
                systemGraphOperations,
                systemGraphInitializer,
                initSystemGraphOnStart,
                secureHasher,
                new BasicPasswordPolicy(),
                createAuthenticationStrategy( config ),
                config.get( SecuritySettings.native_authentication_enabled ),
                config.get( SecuritySettings.native_authorization_enabled )
        );
    }

    private static SystemGraphImportOptions configureImportOptions( Config config, LogProvider logProvider, FileSystemAbstraction fileSystem,
            AccessCapability accessCapability )
    {
        File parentFile = CommunitySecurityModule.getUserRepositoryFile( config ).getParentFile();
        File userImportFile = new File( parentFile, USER_IMPORT_FILENAME );
        File roleImportFile = new File( parentFile, ROLE_IMPORT_FILENAME );

        boolean shouldPerformImport = fileSystem.fileExists( userImportFile ) || fileSystem.fileExists( roleImportFile );
        boolean mayPerformMigration = !shouldPerformImport && mayPerformMigration( accessCapability );
        boolean shouldPurgeImportRepositoriesAfterSuccessfulImport = shouldPerformImport;
        boolean shouldResetSystemGraphAuthBeforeImport = false;

        Supplier<UserRepository> importUserRepositorySupplier = () -> new FileUserRepository( fileSystem, userImportFile, logProvider );
        Supplier<RoleRepository> importRoleRepositorySupplier = () -> new FileRoleRepository( fileSystem, roleImportFile, logProvider );
        Supplier<UserRepository> migrationUserRepositorySupplier = () -> CommunitySecurityModule.getUserRepository( config, logProvider, fileSystem );
        Supplier<RoleRepository> migrationRoleRepositorySupplier = () -> CommercialSecurityModule.getRoleRepository( config, logProvider, fileSystem );
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

    private static SystemGraphImportOptions configureImportOptionsForOfflineImport( UserRepository importUserRepository, RoleRepository importRoleRepository,
            boolean shouldResetSystemGraphAuthBeforeImport )
    {
        boolean shouldPerformImport = true;
        boolean mayPerformMigration = false;
        boolean shouldPurgeImportRepositoriesAfterSuccesfulImport = false;

        Supplier<UserRepository> importUserRepositorySupplier = () -> importUserRepository;
        Supplier<RoleRepository> importRoleRepositorySupplier = () -> importRoleRepository;

        return new SystemGraphImportOptions(
                shouldPerformImport,
                mayPerformMigration,
                shouldPurgeImportRepositoriesAfterSuccesfulImport,
                shouldResetSystemGraphAuthBeforeImport,
                importUserRepositorySupplier,
                importRoleRepositorySupplier,
                /* migrationUserRepositorySupplier = */ null,
                /* migrationRoleRepositorySupplier = */ null,
                /* initialUserRepositorySupplier = */ null,
                /* defaultAdminRepositorySupplier = */ null
        );
    }

    private static boolean mayPerformMigration( AccessCapability accessCapability )
    {
        boolean mayPerformMigration = false;

        // TBD: Should we protect this with a dedicated setting?
        // The argument against using GraphDatabaseSettings.allow_upgrade is that it will also force a potential store upgrade
        //if ( config.get( SecuritySettings.allow_migration ) )
        {
            try
            {
                // Only perform migration if this neo4j instance can write (In a cluster, only the leader can write)
                accessCapability.assertCanWrite();
                mayPerformMigration = true;
            }
            catch ( WriteOperationsNotAllowedException e )
            {
                // Do nothing
            }
        }
        return mayPerformMigration;
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
            for ( AuthPlugin plugin : Service.loadAll( AuthPlugin.class ) )
            {
                PluginRealm pluginRealm =
                        new PluginRealm( plugin, config, securityLog, Clocks.systemClock(), secureHasher );
                availablePluginRealms.add( pluginRealm );
            }
        }

        if ( securityConfig.pluginAuthentication )
        {
            for ( AuthenticationPlugin plugin : Service.loadAll( AuthenticationPlugin.class ) )
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
            for ( AuthorizationPlugin plugin : Service.loadAll( AuthorizationPlugin.class ) )
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

    public static RoleRepository getRoleRepository( Config config, LogProvider logProvider, FileSystemAbstraction fileSystem )
    {
        return new FileRoleRepository( fileSystem, getRoleRepositoryFile( config ), logProvider );
    }

    public static UserRepository getDefaultAdminRepository( Config config, LogProvider logProvider,
            FileSystemAbstraction fileSystem )
    {
        return new FileUserRepository( fileSystem, getDefaultAdminRepositoryFile( config ), logProvider );
    }

    public static File getRoleRepositoryFile( Config config )
    {
        return new File( config.get( DatabaseManagementSystemSettings.auth_store_directory ), ROLE_STORE_FILENAME );
    }

    private static File getDefaultAdminRepositoryFile( Config config )
    {
        return new File( config.get( DatabaseManagementSystemSettings.auth_store_directory ),
                DEFAULT_ADMIN_STORE_FILENAME );
    }

    private static IllegalArgumentException illegalConfiguration( String message )
    {
        return new IllegalArgumentException( "Illegal configuration: " + message );
    }

    protected static class SecurityConfig
    {
        protected final List<String> authProviders;
        public final boolean hasNativeProvider;
        protected final boolean hasLdapProvider;
        protected final List<String> pluginAuthProviders;
        protected final boolean nativeAuthentication;
        protected final boolean nativeAuthorization;
        protected final boolean ldapAuthentication;
        protected final boolean ldapAuthorization;
        protected final boolean pluginAuthentication;
        protected final boolean pluginAuthorization;
        protected final boolean propertyAuthorization;
        private final String propertyAuthMapping;
        final Map<String,List<String>> propertyBlacklist = new HashMap<>();
        protected boolean nativeAuthEnabled;

        protected SecurityConfig( Config config )
        {
            authProviders = config.get( SecuritySettings.auth_providers );
            hasNativeProvider = authProviders.contains( SecuritySettings.NATIVE_REALM_NAME );
            hasLdapProvider = authProviders.contains( SecuritySettings.LDAP_REALM_NAME );
            pluginAuthProviders = authProviders.stream()
                    .filter( r -> r.startsWith( SecuritySettings.PLUGIN_REALM_NAME_PREFIX ) )
                    .collect( Collectors.toList() );

            nativeAuthentication = config.get( SecuritySettings.native_authentication_enabled );
            nativeAuthorization = config.get( SecuritySettings.native_authorization_enabled );
            nativeAuthEnabled = nativeAuthentication || nativeAuthorization;
            ldapAuthentication = config.get( SecuritySettings.ldap_authentication_enabled );
            ldapAuthorization = config.get( SecuritySettings.ldap_authorization_enabled );
            pluginAuthentication = config.get( SecuritySettings.plugin_authentication_enabled );
            pluginAuthorization = config.get( SecuritySettings.plugin_authorization_enabled );
            propertyAuthorization = config.get( SecuritySettings.property_level_authorization_enabled );
            propertyAuthMapping = config.get( SecuritySettings.property_level_authorization_permissions );
        }

        protected void validate()
        {
            if ( !nativeAuthentication && !ldapAuthentication && !pluginAuthentication )
            {
                throw illegalConfiguration( "All authentication providers are disabled." );
            }

            if ( !nativeAuthorization && !ldapAuthorization && !pluginAuthorization )
            {
                throw illegalConfiguration( "All authorization providers are disabled." );
            }

            if ( hasNativeProvider && !nativeAuthentication && !nativeAuthorization )
            {
                throw illegalConfiguration(
                        "Native auth provider configured, but both authentication and authorization are disabled." );
            }

            if ( hasLdapProvider && !ldapAuthentication && !ldapAuthorization )
            {
                throw illegalConfiguration(
                        "LDAP auth provider configured, but both authentication and authorization are disabled." );
            }

            if ( !pluginAuthProviders.isEmpty() && !pluginAuthentication && !pluginAuthorization )
            {
                throw illegalConfiguration(
                        "Plugin auth provider configured, but both authentication and authorization are disabled." );
            }
            if ( propertyAuthorization && !parsePropertyPermissions() )
            {
                throw illegalConfiguration(
                        "Property level authorization is enabled but there is a error in the permissions mapping." );
            }
        }

        protected boolean parsePropertyPermissions()
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

        protected boolean onlyPluginAuthentication()
        {
            return !nativeAuthentication && !ldapAuthentication && pluginAuthentication;
        }

        protected boolean onlyPluginAuthorization()
        {
            return !nativeAuthorization && !ldapAuthorization && pluginAuthorization;
        }
    }

    static class CommercialSecurityConfig extends SecurityConfig
    {
        final boolean hasSystemGraphProvider;

        CommercialSecurityConfig( Config config )
        {
            super( config );
            hasSystemGraphProvider = authProviders.contains( SecuritySettings.SYSTEM_GRAPH_REALM_NAME );
        }

        @Override
        protected void validate()
        {
            if ( hasSystemGraphProvider && !nativeAuthentication && !nativeAuthorization )
            {
                throw illegalConfiguration(
                        "System graph auth provider configured, but both authentication and authorization are disabled." );
            }

            if ( hasNativeProvider && hasSystemGraphProvider )
            {
                throw illegalConfiguration(
                        "Both system graph auth provider and native auth provider configured," +
                                " but they cannot be used together. Please remove one of them from the configuration." );
            }
            super.validate();
        }
    }

    // This is used by ImportAuthCommand for offline import of auth information
    public static SystemGraphRealm createSystemGraphRealmForOfflineImport( Config config,
            SecurityLog securityLog,
            DatabaseManager databaseManager,
            UserRepository importUserRepository, RoleRepository importRoleRepository,
            boolean shouldResetSystemGraphAuthBeforeImport )
    {
        ContextSwitchingSystemGraphQueryExecutor queryExecutor = new ContextSwitchingSystemGraphQueryExecutor( databaseManager, SYSTEM_DATABASE_NAME );
        SecureHasher secureHasher = new SecureHasher();
        SystemGraphImportOptions importOptions =
                configureImportOptionsForOfflineImport( importUserRepository, importRoleRepository, shouldResetSystemGraphAuthBeforeImport );

        SystemGraphOperations systemGraphOperations = new SystemGraphOperations( queryExecutor, secureHasher );
        SystemGraphInitializer systemGraphInitializer =
                new SystemGraphInitializer( queryExecutor, systemGraphOperations, importOptions, secureHasher, securityLog );

        return new SystemGraphRealm(
                systemGraphOperations,
                systemGraphInitializer,
                true,
                new SecureHasher(),
                new BasicPasswordPolicy(),
                createAuthenticationStrategy( config ),
                false,
                true // At least one of these needs to be true for the realm to consider imports
        );
    }
}
