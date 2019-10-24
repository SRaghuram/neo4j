/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.configuration;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;
import org.neo4j.logging.Level;
import org.neo4j.string.SecureString;

import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.neo4j.configuration.SettingConstraints.min;
import static org.neo4j.configuration.SettingConstraints.range;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.BYTES;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.PATH;
import static org.neo4j.configuration.SettingValueParsers.SECURE_STRING;
import static org.neo4j.configuration.SettingValueParsers.STRING;
import static org.neo4j.configuration.SettingValueParsers.listOf;
import static org.neo4j.configuration.SettingValueParsers.ofEnum;

@ServiceProvider
public class SecuritySettings implements SettingsDeclaration
{
    public static final String NATIVE_REALM_NAME = "native";
    public static final String LDAP_REALM_NAME = "ldap";
    public static final String PLUGIN_REALM_NAME_PREFIX = "plugin-";

    //=========================================================================
    // Realm settings
    //=========================================================================

    @Description( "A list of security authentication providers containing the users and roles. " +
            "This can be any of the built-in `" + NATIVE_REALM_NAME + "` or `" + LDAP_REALM_NAME + "` providers, " +
            "or it can be an externally provided plugin, with a custom name prefixed by `" +
            PLUGIN_REALM_NAME_PREFIX + "`, i.e. `" + PLUGIN_REALM_NAME_PREFIX + "<AUTH_PROVIDER_NAME>`. " +
            "They will be queried in the given order when login is attempted." )
    public static final Setting<List<String>> authentication_providers =
            newBuilder( "dbms.security.authentication_providers", listOf( STRING ), List.of( NATIVE_REALM_NAME ) ).build();

    @Description( "A list of security authorization providers containing the users and roles. " +
            "This can be any of the built-in `" + NATIVE_REALM_NAME + "` or `" + LDAP_REALM_NAME + "` providers, " +
            "or it can be an externally provided plugin, with a custom name prefixed by `" +
            PLUGIN_REALM_NAME_PREFIX + "`, i.e. `" + PLUGIN_REALM_NAME_PREFIX + "<AUTH_PROVIDER_NAME>`. " +
            "They will be queried in the given order when login is attempted." )
    public static final Setting<List<String>> authorization_providers =
            newBuilder( "dbms.security.authorization_providers", listOf( STRING ), List.of( NATIVE_REALM_NAME ) ).build();

    //=========================================================================
    // LDAP settings
    //=========================================================================

    @Description( "URL of LDAP server to use for authentication and authorization. " +
            "The format of the setting is `<protocol>://<hostname>:<port>`, where hostname is the only " +
            "required field. The supported values for protocol are `ldap` (default) and `ldaps`. " +
            "The default port for `ldap` is 389 and for `ldaps` 636. For example: " +
            "`ldaps://ldap.example.com:10389`.\n" +
            "You may want to consider using STARTTLS (`dbms.security.ldap.use_starttls`) instead of LDAPS " +
            "for secure connections, in which case the correct protocol is `ldap`." )
    public static final Setting<String> ldap_server =
            newBuilder( "dbms.security.ldap.host", STRING, "localhost" ).build();

    @Description( "Use secure communication with the LDAP server using opportunistic TLS. " +
            "First an initial insecure connection will be made with the LDAP server, and a STARTTLS command will be " +
            "issued to negotiate an upgrade of the connection to TLS before initiating authentication." )
    public static final Setting<Boolean> ldap_use_starttls =
            newBuilder( "dbms.security.ldap.use_starttls", BOOL, false ).build();

    @Description(
            "The LDAP referral behavior when creating a connection. This is one of `follow`, `ignore` or `throw`.\n" +
                    "* `follow` automatically follows any referrals\n" +
                    "* `ignore` ignores any referrals\n" +
                    "* `throw` throws an exception, which will lead to authentication failure" )
    public static final Setting<String> ldap_referral =
            newBuilder( "dbms.security.ldap.referral", STRING, "follow" ).build();

    @Description( "The timeout for establishing an LDAP connection. If a connection with the LDAP server cannot be " +
            "established within the given time the attempt is aborted. " +
            "A value of 0 means to use the network protocol's (i.e., TCP's) timeout value." )
    public static final Setting<Duration> ldap_connection_timeout =
            newBuilder( "dbms.security.ldap.connection_timeout", DURATION, ofSeconds( 30 ) ).build();

    @Description( "The timeout for an LDAP read request (i.e. search). If the LDAP server does not respond within " +
            "the given time the request will be aborted. A value of 0 means wait for a response indefinitely." )
    public static final Setting<Duration> ldap_read_timeout =
            newBuilder( "dbms.security.ldap.read_timeout", DURATION, ofSeconds( 30 ) ).build();

    //-----------------------------------------------------
    // LDAP authentication settings
    //-----------------------------------------------------

    @Description( "LDAP authentication mechanism. This is one of `simple` or a SASL mechanism supported by JNDI, " +
            "for example `DIGEST-MD5`. `simple` is basic username" +
            " and password authentication and SASL is used for more advanced mechanisms. See RFC 2251 LDAPv3 " +
            "documentation for more details." )
    public static final Setting<String> ldap_authentication_mechanism =
            newBuilder( "dbms.security.ldap.authentication.mechanism", STRING, "simple" ).build();

    @Description(
            "LDAP user DN template. An LDAP object is referenced by its distinguished name (DN), and a user DN is " +
                    "an LDAP fully-qualified unique user identifier. This setting is used to generate an LDAP DN that " +
                    "conforms with the LDAP directory's schema from the user principal that is submitted with the " +
                    "authentication token when logging in. The special token {0} is a " +
                    "placeholder where the user principal will be substituted into the DN string." )
    public static final Setting<String> ldap_authentication_user_dn_template =
            newBuilder( "dbms.security.ldap.authentication.user_dn_template", STRING, "uid={0},ou=users,dc=example,dc=com" ).build();

    @Description( "Determines if the result of authentication via the LDAP server should be cached or not. " +
            "Caching is used to limit the number of LDAP requests that have to be made over the network " +
            "for users that have already been authenticated successfully. A user can be authenticated against " +
            "an existing cache entry (instead of via an LDAP server) as long as it is alive " +
            "(see `dbms.security.auth_cache_ttl`).\n" +
            "An important consequence of setting this to `true` is that " +
            "Neo4j then needs to cache a hashed version of the credentials in order to perform credentials " +
            "matching. This hashing is done using a cryptographic hash function together with a random salt. " +
            "Preferably a conscious decision should be made if this method is considered acceptable by " +
            "the security standards of the organization in which this Neo4j instance is deployed." )
    public static final Setting<Boolean> ldap_authentication_cache_enabled =
            newBuilder( "dbms.security.ldap.authentication.cache_enabled", BOOL, true ).build();

    @Description( "Perform authentication with sAMAccountName instead of DN.\n" +
            "Using this setting requires `dbms.security.ldap.authorization.system_username` and " +
            "dbms.security.ldap.authorization.system_password to be used since there is no way to log in " +
            "through ldap directly with the sAMAccountName, instead the login name will be resolved to a DN " +
            "that will be used to log in with." )
    public static final Setting<Boolean> ldap_authentication_use_samaccountname =
            newBuilder( "dbms.security.ldap.authentication.use_samaccountname", BOOL, false ).build();

    //-----------------------------------------------------
    // LDAP authorization settings
    //-----------------------------------------------------

    @Description( "Perform LDAP search for authorization info using a system account instead of the user's own account.\n" +
            "If this is set to `false` (default), the search for group membership will be performed " +
            "directly after authentication using the LDAP context bound with the user's own account. " +
            "The mapped roles will be cached for the duration of `dbms.security.auth_cache_ttl`, " +
            "and then expire, requiring re-authentication. To avoid frequently having to re-authenticate " +
            "sessions you may want to set a relatively long auth cache expiration time together with this " +
            "option. NOTE: This option will only work if the users are permitted to search for their " +
            "own group membership attributes in the directory.\n" +
            "If this is set to `true`, the search will be performed using a special system account user " +
            "with read access to all the users in the directory. " +
            "You need to specify the username and password using the settings " +
            "`dbms.security.ldap.authorization.system_username` and " +
            "`dbms.security.ldap.authorization.system_password` with this option. " +
            "Note that this account only needs read access to the relevant parts of the LDAP directory " +
            "and does not need to have access rights to Neo4j, or any other systems." )
    public static final Setting<Boolean> ldap_authorization_use_system_account =
            newBuilder( "dbms.security.ldap.authorization.use_system_account", BOOL, false ).build();

    @Description(
            "An LDAP system account username to use for authorization searches when " +
                    "`dbms.security.ldap.authorization.use_system_account` is `true`. " +
                    "Note that the `dbms.security.ldap.authentication.user_dn_template` will not be applied to " +
                    "this username, so you may have to specify a full DN." )
    public static final Setting<String> ldap_authorization_system_username =
            newBuilder( "dbms.security.ldap.authorization.system_username", STRING, null ).build();

    @Description( "An LDAP system account password to use for authorization searches when " +
            "`dbms.security.ldap.authorization.use_system_account` is `true`." )
    public static final Setting<SecureString> ldap_authorization_system_password =
            newBuilder( "dbms.security.ldap.authorization.system_password", SECURE_STRING, null ).build();

    @Description( "The name of the base object or named context to search for user objects when " +
            "LDAP authorization is enabled. A common case is that this matches the last part " +
            "of `dbms.security.ldap.authentication.user_dn_template`." )
    public static final Setting<String> ldap_authorization_user_search_base =
            newBuilder( "dbms.security.ldap.authorization.user_search_base", STRING, "ou=users,dc=example,dc=com" ).build();

    @Description( "The LDAP search filter to search for a user principal when LDAP authorization is " +
            "enabled. The filter should contain the placeholder token {0} which will be substituted for the " +
            "user principal." )
    public static final Setting<String> ldap_authorization_user_search_filter =
            newBuilder( "dbms.security.ldap.authorization.user_search_filter", STRING, "(&(objectClass=*)(uid={0}))" ).build();

    @Description( "A list of attribute names on a user object that contains groups to be used for mapping to roles " +
            "when LDAP authorization is enabled." )
    public static final Setting<List<String>> ldap_authorization_group_membership_attribute_names =
            newBuilder( "dbms.security.ldap.authorization.group_membership_attributes", listOf( STRING ), List.of( "memberOf" ) ).build();

    @Description( "An authorization mapping from LDAP group names to Neo4j role names. " +
            "The map should be formatted as a semicolon separated list of key-value pairs, where the " +
            "key is the LDAP group name and the value is a comma separated list of corresponding role names. " +
            "For example: group1=role1;group2=role2;group3=role3,role4,role5\n" +
            "You could also use whitespaces and quotes around group names to make this mapping more readable, " +
            "for example: \n" +
            "----\n" +
            "+dbms.security.ldap.authorization.group_to_role_mapping+=\\\n" +
            "         \"cn=Neo4j Read Only,cn=users,dc=example,dc=com\"      = reader;    \\\n" +
            "         \"cn=Neo4j Read-Write,cn=users,dc=example,dc=com\"     = publisher; \\\n" +
            "         \"cn=Neo4j Schema Manager,cn=users,dc=example,dc=com\" = architect; \\\n" +
            "         \"cn=Neo4j Administrator,cn=users,dc=example,dc=com\"  = admin\n" +
            "----\n" +
            "Deprecated: This will be replaced by dynamic configuration in the system graph in 4.0, " +
            "including a migration step for the existing setting value. +\n" )
    @Deprecated
    public static final Setting<String> ldap_authorization_group_to_role_mapping =
            newBuilder( "dbms.security.ldap.authorization.group_to_role_mapping", STRING, null ).build();

    //=========================================================================
    // Cache settings
    //=========================================================================

    @Description( "The time to live (TTL) for cached authentication and authorization info when using " +
            "external auth providers (LDAP or plugin). Setting the TTL to 0 will disable auth caching. " +
            "Disabling caching while using the LDAP auth provider requires the use of an LDAP system account " +
            "for resolving authorization information." )
    public static final Setting<Duration> auth_cache_ttl =
            newBuilder( "dbms.security.auth_cache_ttl", DURATION, ofMinutes( 10 ) ).build();

    @Description( "Enable time-based eviction of the authentication and authorization info cache for " +
            "external auth providers (LDAP or plugin). Disabling this setting will make the cache " +
            "live forever and only be evicted when `dbms.security.auth_cache_max_capacity` is exceeded." )
    public static final Setting<Boolean> auth_cache_use_ttl =
            newBuilder( "dbms.security.auth_cache_use_ttl", BOOL, true ).build();

    @Description( "The maximum capacity for authentication and authorization caches (respectively)." )
    public static final Setting<Integer> auth_cache_max_capacity =
            newBuilder( "dbms.security.auth_cache_max_capacity", INT, 10000 ).build();

    //=========================================================================
    // Security log settings
    //=========================================================================

    @Description( "Path to the security log file." )
    public static final Setting<Path> security_log_filename = newBuilder( "dbms.logs.security.path", PATH, Path.of( "security.log" ) )
            .immutable()
            .setDependency( GraphDatabaseSettings.logs_directory )
            .build();

    @Description( "Security log level threshold." )
    public static final Setting<Level> security_log_level = newBuilder( "dbms.logs.security.level",
            ofEnum( Level.class ), Level.INFO ).build();

    @Description( "Set to log successful authentication events to the security log. " +
            "If this is set to `false` only failed authentication events will be logged, which " +
            "could be useful if you find that the successful events spam the logs too much, " +
            "and you do not require full auditing capability." )
    public static final Setting<Boolean> security_log_successful_authentication =
            newBuilder( "dbms.security.log_successful_authentication", BOOL, true ).build();

    @Description( "Threshold for rotation of the security log." )
    public static final Setting<Long> store_security_log_rotation_threshold =
            newBuilder( "dbms.logs.security.rotation.size", BYTES, ByteUnit.mebiBytes( 20 ) ).addConstraint( range( 0L, Long.MAX_VALUE ) ).build();

    @Description( "Minimum time interval after last rotation of the security log before it may be rotated again." )
    public static final Setting<Duration> store_security_log_rotation_delay =
            newBuilder( "dbms.logs.security.rotation.delay", DURATION, ofSeconds( 300 ) ).build();

    @Description( "Maximum number of history files for the security log." )
    public static final Setting<Integer> store_security_log_max_archives =
            newBuilder( "dbms.logs.security.rotation.keep_number", INT, 7 ).addConstraint( min(1) ).build();

    //=========================================================================
    // Misc settings
    //=========================================================================

    @Internal
    @Description( "Set to true if connection pooling should be used for authorization searches using the " +
            "system account." )
    public static final Setting<Boolean> ldap_authorization_connection_pooling =
            newBuilder( "unsupported.dbms.security.ldap.authorization.connection_pooling", BOOL, true ).build();

    //=========================================================================
    // Property level security settings
    //=========================================================================

    @Description( "This has been replaced by privilege management on roles. Setting it to true will prevent the server from starting." )
    @Deprecated
    public static final Setting<Boolean> property_level_authorization_enabled =
            newBuilder( "dbms.security.property_level.enabled", BOOL, false ).build();

    @Description( "This can be achieved with `DENY READ {property} ON GRAPH * ELEMENTS * TO role`. " +
                  "Using this setting will prevent the server from starting." )
    @Internal
    @Deprecated
    public static final Setting<String> property_level_authorization_permissions =
            newBuilder( "dbms.security.property_level.blacklist", STRING, null ).build();
}
