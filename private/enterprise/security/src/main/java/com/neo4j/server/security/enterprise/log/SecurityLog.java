/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.log;

import com.neo4j.configuration.SecuritySettings;

import java.util.function.Consumer;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.AbstractLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.LogConfig;
import org.neo4j.logging.log4j.Neo4jLoggerContext;

import static org.neo4j.internal.helpers.Strings.escape;

public class SecurityLog extends AbstractLog implements Lifecycle
{
    private Log inner;
    private Neo4jLoggerContext ctx;
    private Config config;

    public SecurityLog( Config config )
    {
        this.config = config;
    }

    /* Only used for tests */
    public SecurityLog( Log log )
    {
        inner = log;
    }

    @Override
    public void init()
    {
        ctx = LogConfig.createBuilder( config.get( SecuritySettings.security_log_filename ), config.get( SecuritySettings.security_log_level ) )
                .withCategory( false )
                .withFormat( config.get( GraphDatabaseInternalSettings.log_format ) )
                .withTimezone( config.get( GraphDatabaseSettings.db_timezone ) )
                .withRotation( config.get( SecuritySettings.store_security_log_rotation_threshold ),
                        config.get( SecuritySettings.store_security_log_max_archives ) )
                .build();
        LogProvider logProvider = new Log4jLogProvider( ctx );
        this.inner = logProvider.getLog( "" );
    }

    @Override
    public void start()
    {
    }

    @Override
    public void stop()
    {
    }

    @Override
    public void shutdown()
    {
        ctx.close();
    }

    @Override
    public boolean isDebugEnabled()
    {
        return inner.isDebugEnabled();
    }

    @Override
    public void debug( String message )
    {
        inner.debug( message );
    }

    @Override
    public void debug( String message, Throwable throwable )
    {
        inner.debug( message, throwable );
    }

    @Override
    public void debug( String format, Object... arguments )
    {
        inner.debug( format, arguments );
    }

    public void debug( AuthSubject subject, String format, Object... arguments )
    {
        inner.debug( withSubject( subject, format ), arguments );
    }

    @Override
    public void info( String message )
    {
        inner.info( message );
    }

    @Override
    public void info( String message, Throwable throwable )
    {
        inner.info( message, throwable );
    }

    @Override
    public void info( String format, Object... arguments )
    {
        inner.info( format, arguments );
    }

    public void info( AuthSubject subject, String format, Object... arguments )
    {
        inner.info( withSubject( subject, format ), arguments );
    }

    public void info( AuthSubject subject, String format )
    {
        inner.info( withSubject( subject, format ) );
    }

    @Override
    public void warn( String message )
    {
        inner.warn( message );
    }

    @Override
    public void warn( String message, Throwable throwable )
    {
        inner.warn( message, throwable );
    }

    @Override
    public void warn( String format, Object... arguments )
    {
        inner.warn( format, arguments );
    }

    public void warn( AuthSubject subject, String format, Object... arguments )
    {
        inner.warn( withSubject( subject, format ), arguments );
    }

    @Override
    public void error( String message )
    {
        inner.error( message );
    }

    @Override
    public void error( String message, Throwable throwable )
    {
        inner.error( message, throwable );
    }

    @Override
    public void error( String format, Object... arguments )
    {
        inner.error( format, arguments );
    }

    public void error( AuthSubject subject, String format, Object... arguments )
    {
        inner.error( withSubject( subject, format ), arguments );
    }

    @Override
    @Deprecated
    public void bulk( Consumer<Log> consumer )
    {
        inner.bulk( consumer );
    }

    private static String withSubject( AuthSubject subject, String msg )
    {
        return "[" + escape( subject.username() ) + "]: " + msg;
    }
}
