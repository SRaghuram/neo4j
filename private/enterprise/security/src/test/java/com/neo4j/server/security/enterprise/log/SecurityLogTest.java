/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.log;

import com.neo4j.configuration.SecuritySettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Scanner;
import java.util.TimeZone;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogTimeZone;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;


@TestDirectoryExtension
class SecurityLogTest
{
    @Inject
    private DefaultFileSystemAbstraction fs;
    @Inject
    private TestDirectory dir;

    private Config config;

    @BeforeEach
    void setUp()
    {
        config = Config.newBuilder()
                .set( SecuritySettings.store_security_log_rotation_threshold, 5L )
                .set( GraphDatabaseSettings.neo4j_home, dir.homePath() ).build();
    }

    @Test
    void shouldRotateLog() throws Exception
    {
        SecurityLog securityLog = new SecurityLog( config );
        securityLog.init();
        securityLog.info( "line 1" );
        securityLog.info( "line 2" );
        securityLog.shutdown();

        File activeLogFile = config.get( SecuritySettings.security_log_filename ).toFile();
        assertThat( fs.fileExists( activeLogFile.toPath() ) ).isEqualTo( true );
        assertThat( fs.fileExists( archive( 1 ).toPath() ) ).isEqualTo( true );
        assertThat( fs.fileExists( archive( 2 ).toPath() ) ).isEqualTo( false );

        String[] activeLines = readLogFile( fs, activeLogFile );
        assertThat( activeLines ).allMatch( item -> item.contains( "line 2" ) );

        String[] archiveLines = readLogFile( fs, archive( 1 ) );
        assertThat( archiveLines ).allMatch( item -> item.contains( "line 1" ) );
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
        Config timeZoneConfig = Config.newBuilder()
                .set( GraphDatabaseSettings.db_timezone, LogTimeZone.SYSTEM )
                .set( GraphDatabaseSettings.neo4j_home, dir.homePath() ).build();
        SecurityLog securityLog = new SecurityLog( timeZoneConfig );
        securityLog.init();
        securityLog.info( "line 1" );
        securityLog.shutdown();

        File activeLogFile = timeZoneConfig.get( SecuritySettings.security_log_filename ).toFile();
        String[] activeLines = readLogFile( fs, activeLogFile );
        assertThat( activeLines ).allMatch( item -> item.contains( timeZoneSuffix ) );
        dir.cleanup();
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
        var stringValues = List.of( "debug: debug line", "debug: info line", "debug: warn line", "debug: error line", "info: info line", "info: warn line",
                "info: error line", "warn: warn line", "warn: error line", "error: error line" );
        for ( int i = 0; i < activeLines.length; i++ )
        {
            assertThat( activeLines[i] ).contains( stringValues.get( i ) );
        }
    }

    private void writeAllLevelsAndShutdown( SecurityLog securityLog, String tag )
    {
        securityLog.init();
        securityLog.debug( format( "%s: debug line", tag ) );
        securityLog.info( format( "%s: info line", tag ) );
        securityLog.warn( format( "%s: warn line", tag ) );
        securityLog.error( format( "%s: error line", tag ) );
        securityLog.shutdown();
    }

    private SecurityLog withLogLevel( Level debug )
    {
        return new SecurityLog(
                Config.newBuilder().set( SecuritySettings.security_log_level, debug )
                        .set( GraphDatabaseSettings.neo4j_home, dir.homePath() ).build()
            );
    }

    private String[] readLogFile( FileSystemAbstraction fs, File activeLogFile ) throws IOException
    {
        try ( Scanner scan = new Scanner( fs.openAsInputStream( activeLogFile.toPath() ) ) )
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
