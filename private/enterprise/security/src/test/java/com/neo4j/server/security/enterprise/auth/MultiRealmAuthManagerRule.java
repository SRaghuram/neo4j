/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.InMemoryUserManager;
import org.apache.shiro.cache.MemoryConstrainedCacheManager;
import org.junit.After;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.StringWriter;
import java.util.Collections;

import org.neo4j.configuration.Config;
import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.Log;

import static org.junit.Assert.fail;

public class MultiRealmAuthManagerRule implements TestRule
{
    private MultiRealmAuthManager manager;
    private InMemoryUserManager realm;

    private void setupAuthManager() throws Throwable
    {
        FormattedLog.Builder builder = FormattedLog.withUTCTimeZone();
        StringWriter securityLogWriter = new StringWriter();
        Log log = builder.toWriter( securityLogWriter );
        SecurityLog securityLog = new SecurityLog( log );
        realm = new InMemoryUserManager( Config.defaults() );

        manager = new MultiRealmAuthManager( realm, Collections.singleton( realm ), new MemoryConstrainedCacheManager(), securityLog, true );
        manager.init();
    }

    InMemoryUserManager getUserManager()
    {
        return realm;
    }

    EnterpriseAuthManager getManager()
    {
        return manager;
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
                    setupAuthManager();
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
}
