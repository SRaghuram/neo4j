/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.transaction_logs_root_path;

public class OnlineBackupContextFactoryTest
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public ExpectedException expected = ExpectedException.none();
    @Rule
    public SuppressOutput suppress = SuppressOutput.suppressAll();

    private Path homeDir;
    private Path configDir;
    private Path configFile;

    @Before
    public void setUp() throws IOException
    {
        homeDir = testDirectory.directory( "home" ).toPath();
        configDir = testDirectory.directory( "config" ).toPath();
        configFile = configDir.resolve( "neo4j.conf" );
        String neo4jConfContents = "dbms.backup.address = localhost:1234";
        Files.write( configFile, singletonList( neo4jConfContents ) );
    }

    @Test
    public void unspecifiedHostnameIsEmptyOptional() throws Exception
    {
        OnlineBackupContextFactory handler = new OnlineBackupContextFactory( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( requiredAnd( "--from=:1234" ) );
        OnlineBackupRequiredArguments requiredArguments = context.getRequiredArguments();

        assertFalse( requiredArguments.getAddress().getHostname().isPresent() );
        assertEquals( 1234, requiredArguments.getAddress().getPort().get().intValue() );
    }

    @Test
    public void unspecifiedPortIsEmptyOptional() throws Exception
    {
        OnlineBackupContextFactory handler = new OnlineBackupContextFactory( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( requiredAnd( "--from=abc" ) );
        OnlineBackupRequiredArguments requiredArguments = context.getRequiredArguments();

        assertEquals( "abc", requiredArguments.getAddress().getHostname().get() );
        assertFalse( requiredArguments.getAddress().getPort().isPresent() );
    }

    @Test
    public void acceptHostWithTrailingPort() throws Exception
    {
        OnlineBackupContextFactory handler = new OnlineBackupContextFactory( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( requiredAnd( "--from=foo.bar.server:" ) );
        OnlineBackupRequiredArguments requiredArguments = context.getRequiredArguments();
        assertEquals( "foo.bar.server", requiredArguments.getAddress().getHostname().get() );
        assertFalse( requiredArguments.getAddress().getPort().isPresent() );
    }

    @Test
    public void acceptPortWithPrecedingEmptyHost() throws Exception
    {
        OnlineBackupContextFactory handler = new OnlineBackupContextFactory( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( requiredAnd( "--from=:1234" ) );
        OnlineBackupRequiredArguments requiredArguments = context.getRequiredArguments();
        assertFalse( requiredArguments.getAddress().getHostname().isPresent() );
        assertEquals( 1234, requiredArguments.getAddress().getPort().get().intValue() );
    }

    @Test
    public void acceptBothIfSpecified() throws Exception
    {
        OnlineBackupContextFactory handler = new OnlineBackupContextFactory( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( requiredAnd( "--from=foo.bar.server:1234" ) );
        OnlineBackupRequiredArguments requiredArguments = context.getRequiredArguments();
        assertEquals( "foo.bar.server", requiredArguments.getAddress().getHostname().get() );
        assertEquals( 1234, requiredArguments.getAddress().getPort().get().intValue() );
    }

    @Test
    public void backupDirectoryArgumentIsMandatory() throws Exception
    {
        expected.expect( IncorrectUsage.class );
        expected.expectMessage( "Missing argument 'backup-dir'" );
        new OnlineBackupContextFactory( homeDir, configDir ).createContext();
    }

    @Test
    public void shouldDefaultTimeoutToTwentyMinutes() throws Exception
    {
        OnlineBackupContextFactory handler = new OnlineBackupContextFactory( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( "--backup-dir=/", "--name=mybackup" );
        OnlineBackupRequiredArguments requiredArguments = context.getRequiredArguments();

        assertEquals( Duration.ofMinutes( 20 ), requiredArguments.getTimeout() );
    }

    @Test
    public void shouldInterpretAUnitlessTimeoutAsSeconds() throws Exception
    {
        OnlineBackupContextFactory handler = new OnlineBackupContextFactory( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( "--timeout=10", "--backup-dir=/", "--name=mybackup" );
        OnlineBackupRequiredArguments requiredArguments = context.getRequiredArguments();

        assertEquals( Duration.ofSeconds( 10 ), requiredArguments.getTimeout() );
    }

    @Test
    public void shouldParseATimeoutWithUnits() throws Exception
    {
        OnlineBackupContextFactory handler = new OnlineBackupContextFactory( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( requiredAnd( "--timeout=10h" ) );
        OnlineBackupRequiredArguments requiredArguments = context.getRequiredArguments();

        assertEquals( Duration.ofHours( 10 ), requiredArguments.getTimeout() );
    }

    @Test
    public void shouldTreatNameArgumentAsMandatory() throws Exception
    {
        expected.expect( IncorrectUsage.class );
        expected.expectMessage( "Missing argument 'name'" );

        OnlineBackupContextFactory handler = new OnlineBackupContextFactory( homeDir, configDir );
        handler.createContext( "--backup-dir=/" );
    }

    @Test
    public void reportDirMustBeAPath() throws Exception
    {
        expected.expect( IncorrectUsage.class );
        expected.expectMessage( "cc-report-dir must be a path" );
        OnlineBackupContextFactory handler = new OnlineBackupContextFactory( homeDir, configDir );
        handler.createContext( requiredAnd( "--check-consistency", "--cc-report-dir" ) );
    }

    @Test
    public void errorHandledForNonExistingAdditionalConfigFile() throws Exception
    {
        // given
        Path additionalConf = homeDir.resolve( "neo4j.conf" );

        // and
        expected.expect( CommandFailed.class );
        expected.expectMessage( containsString( "does not exist" ) );

        // expect
        OnlineBackupContextFactory handler = new OnlineBackupContextFactory( homeDir, configDir );
        handler.createContext( requiredAnd( "--additional-config=" + additionalConf ) );
    }

    @Test
    public void prioritiseConfigDirOverHomeDir() throws Exception
    {
        // given
        Files.write( configFile, singletonList( "causal_clustering.minimum_core_cluster_size_at_startup=4" ), WRITE );

        // and
        Path homeDirConfigFile = homeDir.resolve( "neo4j.conf" );
        Files.write( homeDirConfigFile, asList( "causal_clustering.minimum_core_cluster_size_at_startup=5", "causal_clustering.raft_in_queue_max_batch=21" ) );

        // when
        OnlineBackupContextFactory handler = new OnlineBackupContextFactory( homeDir, configDir );
        Config config = handler.createContext( requiredAnd() ).getConfig();

        // then
        assertEquals( Integer.valueOf( 3 ), config.get( CausalClusteringSettings.minimum_core_cluster_size_at_formation ) );
        assertEquals( Integer.valueOf( 128 ), config.get( CausalClusteringSettings.raft_in_queue_max_batch ) );
    }

    @Test
    public void prioritiseAdditionalOverConfigDir() throws Exception
    {
        // given
        Files.write( configFile, asList( "causal_clustering.minimum_core_cluster_size_at_startup=4", "causal_clustering.raft_in_queue_max_batch=21" ) );

        // and
        Path additionalConf = homeDir.resolve( "additional-neo4j.conf" );
        Files.write( additionalConf, singletonList( "causal_clustering.minimum_core_cluster_size_at_startup=5" ) );

        // when
        OnlineBackupContextFactory handler = new OnlineBackupContextFactory( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( requiredAnd( "--additional-config=" + additionalConf ) );
        Config config = context.getConfig();

        // then
        assertEquals( Integer.valueOf( 3 ), config.get( CausalClusteringSettings.minimum_core_cluster_size_at_formation ) );
        assertEquals( Integer.valueOf( 21 ), config.get( CausalClusteringSettings.raft_in_queue_max_batch ) );
    }

    @Test
    public void mustIgnorePageCacheConfigInConfigFile() throws Exception
    {
        // given
        Files.write( configFile, singletonList( pagecache_memory.name() + "=42m" ) );

        // when
        OnlineBackupContextFactory contextBuilder = new OnlineBackupContextFactory( homeDir, configDir );
        OnlineBackupContext context = contextBuilder.createContext( requiredAnd() );

        // then
        assertThat( context.getConfig().get( pagecache_memory ), is( "8m" ) );
    }

    @Test
    public void mustIgnorePageCacheConfigInAdditionalConfigFile() throws Exception
    {
        // given
        Path additionalConf = homeDir.resolve( "additional-neo4j.conf" );
        Files.write( additionalConf, singletonList( pagecache_memory.name() + "=42m" ) );

        // when
        OnlineBackupContextFactory builder = new OnlineBackupContextFactory( homeDir, configDir );
        OnlineBackupContext context = builder.createContext( requiredAnd( "--additional-config=" + additionalConf ) );

        // then
        assertThat( context.getConfig().get( pagecache_memory ), is( "8m" ) );
    }

    @Test
    public void mustRespectPageCacheConfigFromCommandLineArguments() throws Exception
    {
        // when
        OnlineBackupContextFactory builder = new OnlineBackupContextFactory( homeDir, configDir );
        OnlineBackupContext context = builder.createContext( requiredAnd( "--pagecache=42m" ) );

        // then
        assertThat( context.getConfig().get( pagecache_memory ), is( "42m" ) );
    }

    @Test
    public void logsMustBePlacedInTargetBackupDirectory() throws Exception
    {
        // when
        String name = "mybackup";
        Path backupDir = homeDir.resolve( "poke" );
        Path backupPath = backupDir.resolve( name );
        Files.createDirectories( backupDir );
        OnlineBackupContextFactory builder = new OnlineBackupContextFactory( homeDir, configDir );
        OnlineBackupContext context = builder.createContext( "--backup-dir=" + backupDir, "--name=" + name );
        assertThat( context.getConfig().get( transaction_logs_root_path ).getAbsolutePath(), is( backupPath.toString() ) );
    }

    @Test
    public void prometheusShouldBeDisabledToAvoidPortConflicts() throws CommandFailed, IncorrectUsage
    {
        OnlineBackupContext context = new OnlineBackupContextFactory( homeDir, configDir ).createContext( requiredAnd() );
        assertEquals( Settings.FALSE, context.getConfig().getRaw().get( "metrics.prometheus.enabled" ) );
    }

    @Test
    public void ipv6CanBeProcessed() throws CommandFailed, IncorrectUsage
    {
        // given
        OnlineBackupContextFactory builder = new OnlineBackupContextFactory( homeDir, configDir );

        // when
        OnlineBackupContext context = builder.createContext( requiredAnd( "--from=[fd00:ce10::2]:6362" ) );

        // then
        assertEquals( "fd00:ce10::2", context.getRequiredArguments().getAddress().getHostname().get() );
        assertEquals( Integer.valueOf( 6362 ), context.getRequiredArguments().getAddress().getPort().get() );
    }

    private String[] requiredAnd( String... additionalArgs )
    {
        List<String> args = new ArrayList<>();
        args.add( "--backup-dir=/" );
        args.add( "--name=mybackup" );
        Collections.addAll( args, additionalArgs );
        return args.toArray( new String[0] );
    }
}
