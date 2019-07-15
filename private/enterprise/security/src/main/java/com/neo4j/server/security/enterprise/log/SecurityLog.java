/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.log;

import com.neo4j.server.security.enterprise.configuration.SecuritySettings;

import java.io.File;
import java.io.IOException;
import java.time.ZoneId;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;
import org.neo4j.logging.RotatingFileOutputStreamSupplier;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.internal.helpers.Strings.escape;

public class SecurityLog extends LifecycleAdapter implements Log
{
    private RotatingFileOutputStreamSupplier rotatingSupplier;
    private final Log inner;

    public SecurityLog( Config config, FileSystemAbstraction fileSystem, Executor executor ) throws IOException
    {
        ZoneId logTimeZoneId = config.get( GraphDatabaseSettings.db_timezone ).getZoneId();
        File logFile = config.get( SecuritySettings.security_log_filename ).toFile();

        FormattedLog.Builder builder = FormattedLog.withZoneId( logTimeZoneId );

        rotatingSupplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile,
                config.get( SecuritySettings.store_security_log_rotation_threshold ),
                config.get( SecuritySettings.store_security_log_rotation_delay ).toMillis(),
                config.get( SecuritySettings.store_security_log_max_archives ), executor );

        FormattedLog formattedLog = builder.toOutputStream( rotatingSupplier );
        formattedLog.setLevel( config.get( SecuritySettings.security_log_level ) );

        this.inner = formattedLog;
    }

    /* Only used for tests */
    public SecurityLog( Log log )
    {
        inner = log;
    }

    private static String withSubject( AuthSubject subject, String msg )
    {
        return "[" + escape( subject.username() ) + "]: " + msg;
    }

    @Override
    public boolean isDebugEnabled()
    {
        return inner.isDebugEnabled();
    }

    @Override
    public Logger debugLogger()
    {
        return inner.debugLogger();
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
    public Logger infoLogger()
    {
        return inner.infoLogger();
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
    public Logger warnLogger()
    {
        return inner.warnLogger();
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
    public Logger errorLogger()
    {
        return inner.errorLogger();
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
    public void bulk( Consumer<Log> consumer )
    {
        inner.bulk( consumer );
    }

    public static SecurityLog create( Config config, FileSystemAbstraction fileSystem,
            JobScheduler jobScheduler ) throws IOException
    {
        try
        {
            return new SecurityLog( config, fileSystem,
                    jobScheduler.executor( Group.LOG_ROTATION ) );
        }
        catch ( SecurityException e )
        {
            throw new IOException( "Unable to create security log.", e );
        }
    }

    @Override
    public void shutdown() throws Exception
    {
        if ( this.rotatingSupplier != null )
        {
            this.rotatingSupplier.close();
            this.rotatingSupplier = null;
        }
    }
}
