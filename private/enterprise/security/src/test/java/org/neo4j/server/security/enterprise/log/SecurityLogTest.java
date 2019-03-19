/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.log;

import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.File;
import java.io.IOException;
import java.time.ZoneOffset;
import java.util.Scanner;
import java.util.TimeZone;

import org.neo4j.adversaries.ClassGuardedAdversary;
import org.neo4j.adversaries.CountingAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogTimeZone;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.array;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.Settings.TRUE;

public class SecurityLogTest
{
    @Rule
    public EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    private Config config = Config.defaults(
            stringMap( SecuritySettings.store_security_log_rotation_threshold.name(), "5",
                    SecuritySettings.store_security_log_rotation_delay.name(), "1ms" ) );

    @Test
    public void shouldRotateLog() throws IOException
    {
        SecurityLog securityLog = new SecurityLog( config, fileSystemRule.get(), Runnable::run );
        securityLog.info( "line 1" );
        securityLog.info( "line 2" );

        FileSystemAbstraction fs = fileSystemRule.get();

        File activeLogFile = config.get( SecuritySettings.security_log_filename );
        assertThat( fs.fileExists( activeLogFile ), equalTo( true ) );
        assertThat( fs.fileExists( archive( 1 ) ), equalTo( true ) );
        assertThat( fs.fileExists( archive( 2 ) ), equalTo( false ) );

        String[] activeLines = readLogFile( fs, activeLogFile );
        assertThat( activeLines, array( containsString( "line 2" ) ) );

        String[] archiveLines = readLogFile( fs, archive( 1 ) );
        assertThat( archiveLines, array( containsString( "line 1" ) ) );
    }

    @Test
    public void shouldFailDatabaseCreationIfNotAbleToCreateSecurityLog()
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();

        // Will throw either an IOException or a runtime SecurityException when calling RotatingFileOutputStreamSupplier.openOutputFile()
        ClassGuardedAdversary adversary = new ClassGuardedAdversary( new CountingAdversary( 1, true ), SecurityLog.class );
        final AdversarialFileSystemAbstraction evilFileSystem = new AdversarialFileSystemAbstraction( adversary );

        final GraphDatabaseBuilder builder =
                new TestEnterpriseGraphDatabaseFactory()
                        .setFileSystem( evilFileSystem )
                        .setInternalLogProvider( logProvider )
                        .newImpermanentDatabaseBuilder()
                        .setConfig( GraphDatabaseSettings.auth_enabled, TRUE );

        // When
        RuntimeException runtimeException =
                Assertions.assertThrows( RuntimeException.class, builder::newGraphDatabase );

        // Then
        assertThat( runtimeException.getMessage(), equalTo( "Failed to load security module.") );
        assertThat( runtimeException.getCause(), instanceOf( IOException.class ) );
        assertThat( runtimeException.getCause().getMessage(), equalTo( "Unable to create security log." ) );

        logProvider.assertContainsMessageMatching( is( "Failed to load security module. Caused by: Unable to create security log." ) );
    }

    @Test
    public void logUseSystemTimeZoneIfConfigured() throws Exception
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
        Config timeZoneConfig = Config.defaults( GraphDatabaseSettings.db_timezone, LogTimeZone.SYSTEM.name() );
        SecurityLog securityLog = new SecurityLog( timeZoneConfig, fileSystemRule.get(), Runnable::run );
        securityLog.info( "line 1" );

        FileSystemAbstraction fs = fileSystemRule.get();
        File activeLogFile = timeZoneConfig.get( SecuritySettings.security_log_filename );
        String[] activeLines = readLogFile( fs, activeLogFile );
        assertThat( activeLines, array( containsString( timeZoneSuffix ) ) );
        fileSystemRule.clear();
    }

    @Test
    public void shouldHonorLogLevel() throws Throwable
    {
        writeAllLevelsAndShutdown( withLogLevel( Level.DEBUG ), "debug" );
        writeAllLevelsAndShutdown( withLogLevel( Level.INFO ), "info" );
        writeAllLevelsAndShutdown( withLogLevel( Level.WARN ), "warn" );
        writeAllLevelsAndShutdown( withLogLevel( Level.ERROR ), "error" );

        FileSystemAbstraction fs = fileSystemRule.get();
        File activeLogFile = config.get( SecuritySettings.security_log_filename );
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
                Config.defaults( SecuritySettings.security_log_level, debug.name() ),
                fileSystemRule.get(),
                Runnable::run
            );
    }

    private String[] readLogFile( FileSystemAbstraction fs, File activeLogFile ) throws IOException
    {
        Scanner scan = new Scanner( fs.openAsInputStream( activeLogFile ) );
        scan.useDelimiter( "\\Z" );
        String allLines = scan.next();
        scan.close();
        return allLines.split( "\\n" );
    }

    private File archive( int archiveNumber )
    {
        return new File( format( "%s.%d", config.get( SecuritySettings.security_log_filename ), archiveNumber ) );
    }
}
