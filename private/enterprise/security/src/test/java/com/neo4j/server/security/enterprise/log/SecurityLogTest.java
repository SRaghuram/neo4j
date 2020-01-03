/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.log;

import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.Scanner;
import java.util.TimeZone;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogTimeZone;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.array;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@EphemeralTestDirectoryExtension
class SecurityLogTest
{
    @Inject
    private EphemeralFileSystemAbstraction fs;

    private final Config config = Config.newBuilder()
            .set( SecuritySettings.store_security_log_rotation_threshold, 5L )
            .set( SecuritySettings.store_security_log_rotation_delay, Duration.ofMillis( 1 ) ).build();

    @Test
    void shouldRotateLog() throws IOException
    {
        SecurityLog securityLog = new SecurityLog( config, fs, Runnable::run );
        securityLog.info( "line 1" );
        securityLog.info( "line 2" );

        File activeLogFile = config.get( SecuritySettings.security_log_filename ).toFile();
        assertThat( fs.fileExists( activeLogFile ), equalTo( true ) );
        assertThat( fs.fileExists( archive( 1 ) ), equalTo( true ) );
        assertThat( fs.fileExists( archive( 2 ) ), equalTo( false ) );

        String[] activeLines = readLogFile( fs, activeLogFile );
        assertThat( activeLines, array( containsString( "line 2" ) ) );

        String[] archiveLines = readLogFile( fs, archive( 1 ) );
        assertThat( archiveLines, array( containsString( "line 1" ) ) );
    }

    @Test
    void logUseSystemTimeZoneIfConfigured() throws Exception
    {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        try
        {
            checkLogTimeZone( 4, "+0400" );
            checkLogTimeZone( -8, "-0800" );
        }
        finally
        {
            TimeZone.setDefault( defaultTimeZone );
        }
    }

    private void checkLogTimeZone( int hoursShift, String timeZoneSuffix ) throws Exception
    {
        TimeZone.setDefault( TimeZone.getTimeZone( ZoneOffset.ofHours( hoursShift ) ) );
        Config timeZoneConfig = Config.defaults( GraphDatabaseSettings.db_timezone, LogTimeZone.SYSTEM );
        SecurityLog securityLog = new SecurityLog( timeZoneConfig, fs, Runnable::run );
        securityLog.info( "line 1" );

        File activeLogFile = timeZoneConfig.get( SecuritySettings.security_log_filename ).toFile();
        String[] activeLines = readLogFile( fs, activeLogFile );
        assertThat( activeLines, array( containsString( timeZoneSuffix ) ) );
        fs.clear();
    }

    @Test
    void shouldHonorLogLevel() throws Throwable
    {
        writeAllLevelsAndShutdown( withLogLevel( Level.DEBUG ), "debug" );
        writeAllLevelsAndShutdown( withLogLevel( Level.INFO ), "info" );
        writeAllLevelsAndShutdown( withLogLevel( Level.WARN ), "warn" );
        writeAllLevelsAndShutdown( withLogLevel( Level.ERROR ), "error" );

        File activeLogFile = config.get( SecuritySettings.security_log_filename ).toFile();
        String[] activeLines = readLogFile( fs, activeLogFile );
        assertThat( activeLines, array(
                containsString( "debug: debug line" ),
                containsString( "debug: info line" ),
                containsString( "debug: warn line" ),
                containsString( "debug: error line" ),

                containsString( "info: info line" ),
                containsString( "info: warn line" ),
                containsString( "info: error line" ),

                containsString( "warn: warn line" ),
                containsString( "warn: error line" ),

                containsString( "error: error line" )
            ) );
    }

    private void writeAllLevelsAndShutdown( SecurityLog securityLog, String tag ) throws Throwable
    {
        securityLog.debug( format( "%s: debug line", tag ) );
        securityLog.info( format( "%s: info line", tag ) );
        securityLog.warn( format( "%s: warn line", tag ) );
        securityLog.error( format( "%s: error line", tag ) );
        securityLog.shutdown();
    }

    private SecurityLog withLogLevel( Level debug ) throws IOException
    {
        return new SecurityLog(
                Config.defaults( SecuritySettings.security_log_level, debug ),
                fs,
                Runnable::run
            );
    }

    private String[] readLogFile( FileSystemAbstraction fs, File activeLogFile ) throws IOException
    {
        try ( Scanner scan = new Scanner( fs.openAsInputStream( activeLogFile ) ) )
        {
            scan.useDelimiter( "\\Z" );
            String allLines = scan.next();
            return allLines.split( "\\n" );
        }
    }

    private File archive( int archiveNumber )
    {
        return new File( format( "%s.%d", config.get( SecuritySettings.security_log_filename ), archiveNumber ) );
    }
}
