/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin;

import com.neo4j.server.security.enterprise.auth.plugin.api.AuthProviderOperations;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import org.junit.jupiter.api.Test;

import java.time.Clock;

import org.neo4j.configuration.Config;
import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.logging.AssertableLogProvider;

import static java.lang.String.format;
import static org.mockito.Mockito.mock;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;

class PluginRealmTest
{
    private final Config config = mock( Config.class );
    private final AssertableLogProvider log = new AssertableLogProvider();
    private final SecurityLog securityLog = new SecurityLog( log.getLog( this.getClass() ) );

    @Test
    void shouldLogToSecurityLogFromAuthPlugin() throws Throwable
    {
        PluginRealm pluginRealm = new PluginRealm( new LoggingAuthPlugin(), config, securityLog, Clock.systemUTC(),
                mock( SecureHasher.class ) );
        pluginRealm.initialize();
        assertLogged( "LoggingAuthPlugin" );
    }

    @Test
    void shouldLogToSecurityLogFromAuthenticationPlugin() throws Throwable
    {
        PluginRealm pluginRealm = new PluginRealm( new LoggingAuthenticationPlugin(),
                null,
                config, securityLog, Clock.systemUTC(), mock( SecureHasher.class ) );
        pluginRealm.initialize( );
        assertLogged( "LoggingAuthenticationPlugin" );
    }

    @Test
    void shouldLogToSecurityLogFromAuthorizationPlugin() throws Throwable
    {
        PluginRealm pluginRealm = new PluginRealm(
                null, new LoggingAuthorizationPlugin(),
                config, securityLog, Clock.systemUTC(), mock( SecureHasher.class ) );
        pluginRealm.initialize();
        assertLogged( "LoggingAuthorizationPlugin" );
    }

    private void assertLogged( String name )
    {
        var matcher = assertThat( log ).forClass( getClass() );
        matcher.forLevel( INFO ).containsMessages( format( "{plugin-%s} info line", name ) );
        matcher.forLevel( WARN ).containsMessages( format( "{plugin-%s} warn line", name ) );
        matcher.forLevel( ERROR ).containsMessages( format( "{plugin-%s} error line", name ) );
    }

    private static class LoggingAuthPlugin extends TestAuthPlugin
    {
        @Override
        public void initialize( AuthProviderOperations api )
        {
            logLines( api );
        }
    }

    private static class LoggingAuthenticationPlugin extends TestAuthenticationPlugin
    {
        @Override
        public void initialize( AuthProviderOperations api )
        {
            logLines( api );
        }
    }

    private static class LoggingAuthorizationPlugin extends TestAuthorizationPlugin
    {
        @Override
        public void initialize( AuthProviderOperations api )
        {
            logLines( api );
        }
    }

    private static void logLines( AuthProviderOperations api )
    {
        AuthProviderOperations.Log log = api.log();
        if ( log.isDebugEnabled() )
        {
            log.debug( "debug line" );
        }
        log.info( "info line" );
        log.warn( "warn line" );
        log.error( "error line" );
    }
}
