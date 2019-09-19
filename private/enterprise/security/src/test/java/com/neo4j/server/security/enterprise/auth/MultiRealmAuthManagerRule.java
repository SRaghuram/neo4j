/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.InMemoryUserManager;
import org.apache.shiro.cache.MemoryConstrainedCacheManager;
import org.junit.After;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.StringWriter;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.auth.SecureHasher;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.fail;

public class MultiRealmAuthManagerRule implements TestRule
{
    private AuthenticationStrategy authStrategy;
    private MultiRealmAuthManager manager;
    private StringWriter securityLogWriter;

    public MultiRealmAuthManagerRule()
    {
        this.authStrategy = new RateLimitedAuthenticationStrategy( Clock.systemUTC(), Config.defaults() );
    }

    private void setupAuthManager( AuthenticationStrategy authStrategy ) throws Throwable
    {
        FormattedLog.Builder builder = FormattedLog.withUTCTimeZone();
        securityLogWriter = new StringWriter();
        Log log = builder.toWriter( securityLogWriter );
        SecureHasher secureHasher = new SecureHasher();
        SecurityLog securityLog = new SecurityLog( log );
        InMemoryUserManager realm = new InMemoryUserManager( Config.defaults(), secureHasher );

        manager = new MultiRealmAuthManager( realm, Collections.singleton( realm ),
                new MemoryConstrainedCacheManager(), securityLog, true, false, Collections.emptyMap() );
        manager.init();
    }

    public EnterpriseAuthAndUserManager getManager()
    {
        return manager;
    }

    public LoginContext makeLoginContext( ShiroSubject shiroSubject )
    {
        return new StandardEnterpriseLoginContext( manager, shiroSubject );
    }

    @Override
    public Statement apply( final Statement base, final Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate()
            {
                try
                {
                    setupAuthManager( authStrategy );
                    base.evaluate();
                }
                catch ( Throwable t )
                {
                    fail( "Got unexpected exception " + t );
                }
                finally
                {
                    try
                    {
                        tearDown();
                    }
                    catch ( Throwable t )
                    {
                        throw new RuntimeException( "Failed to shut down MultiRealmAuthManager", t );
                    }
                }
            }
        };
    }

    @After
    public void tearDown() throws Throwable
    {
        manager.stop();
        manager.shutdown();
    }

    public FullSecurityLog getFullSecurityLog()
    {
        return new FullSecurityLog( securityLogWriter.getBuffer().toString().split( "\n" ) );
    }

    public static class FullSecurityLog
    {
        List<String> lines;

        private FullSecurityLog( String[] logLines )
        {
            lines = Arrays.asList( logLines );
        }

        public void assertHasLine( String subject, String msg )
        {
            assertThat( lines, hasItem( containsString( "[" + subject + "]: " + msg ) ) );
        }
    }
}
