/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.DEFAULT_BACKUP_PORT;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.backup.impl.OnlineBackupContextFactory.DEFAULT_BACKUP_HOSTNAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_warmup_enabled;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.transaction_logs_root_path;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
class OnlineBackupContextFactoryTest
{
    @Inject
    private TestDirectory testDirectory;

    private Path homeDir;
    private Path configDir;
    private Path configFile;

    @BeforeEach
    void setUp() throws IOException
    {
        homeDir = testDirectory.directory( "home" ).toPath();
        configDir = testDirectory.directory( "config" ).toPath();
        configFile = configDir.resolve( "neo4j.conf" );
        String neo4jConfContents = "dbms.backup.listen_address = localhost:1234";
        Files.write( configFile, singletonList( neo4jConfContents ) );
    }

    @Test
    void unspecifiedHostnameFallsBackToDefault() throws Exception
    {
        OnlineBackupContextFactory handler = new OnlineBackupContextFactory( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( requiredAnd( "--from=:1234" ) );
        OnlineBackupRequiredArguments requiredArguments = context.getRequiredArguments();

        assertEquals( DEFAULT_BACKUP_HOSTNAME, requiredArguments.getAddress().getHostname() );
        assertEquals( 1234, requiredArguments.getAddress().getPort() );
    }

    @Test
    void unspecifiedPortFallsBackToDefault() throws Exception
    {
        OnlineBackupContextFactory handler = new OnlineBackupContextFactory( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( requiredAnd( "--from=abc" ) );
        OnlineBackupRequiredArguments requiredArguments = context.getRequiredArguments();

        assertEquals( "abc", requiredArguments.getAddress().getHostname() );
        assertEquals( DEFAULT_BACKUP_PORT, requiredArguments.getAddress().getPort() );
    }

    @Test
    void acceptHostWithTrailingPort() throws Exception
    {
        OnlineBackupContextFactory handler = new OnlineBackupContextFactory( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( requiredAnd( "--from=foo.bar.server:" ) );
        OnlineBackupRequiredArguments requiredArguments = context.getRequiredArguments();

        assertEquals( "foo.bar.server", requiredArguments.getAddress().getHostname() );
        assertEquals( DEFAULT_BACKUP_PORT, requiredArguments.getAddress().getPort() );
    }

    @Test
    void acceptPortWithPrecedingEmptyHost() throws Exception
    {
        OnlineBackupContextFactory handler = new OnlineBackupContextFactory( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( requiredAnd( "--from=:1234" ) );
        OnlineBackupRequiredArguments requiredArguments = context.getRequiredArguments();

        assertEquals( DEFAULT_BACKUP_HOSTNAME, requiredArguments.getAddress().getHostname() );
        assertEquals( 1234, requiredArguments.getAddress().getPort() );
    }

    @Test
    void acceptBothIfSpecified() throws Exception
    {
        OnlineBackupContextFactory handler = new OnlineBackupContextFactory( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( requiredAnd( "--from=foo.bar.server:1234" ) );
        OnlineBackupRequiredArguments requiredArguments = context.getRequiredArguments();

        assertEquals( "foo.bar.server", requiredArguments.getAddress().getHostname() );
        assertEquals( 1234, requiredArguments.getAddress().getPort() );
    }

    @Test
    void backupDirectoryArgumentIsMandatory()
    {
        IncorrectUsage error = assertThrows( IncorrectUsage.class, () -> new OnlineBackupContextFactory( homeDir, configDir ).createContext() );
        assertThat( error.getMessage(), containsString( "Missing argument 'backup-dir'" ) );
    }

    @Test
    void shouldTreatNameArgumentAsMandatory()
    {
        OnlineBackupContextFactory handler = new OnlineBackupContextFactory( homeDir, configDir );

        IncorrectUsage error = assertThrows( IncorrectUsage.class, () -> handler.createContext( "--backup-dir=/" ) );

        assertThat( error.getMessage(), containsString( "Missing argument 'name'" ) );
    }

    @Test
    void reportDirMustBeAPath()
    {
        OnlineBackupContextFactory handler = new OnlineBackupContextFactory( homeDir, configDir );

        IncorrectUsage error = assertThrows( IncorrectUsage.class, () -> handler.createContext( requiredAnd( "--check-consistency", "--cc-report-dir" ) ) );

        assertThat( error.getMessage(), containsString( "cc-report-dir must be a path" ) );
    }

    @Test
    void errorHandledForNonExistingAdditionalConfigFile()
    {
        // given
        OnlineBackupContextFactory handler = new OnlineBackupContextFactory( homeDir, configDir );
        Path additionalConf = homeDir.resolve( "neo4j.conf" );

        // when
        CommandFailed error = assertThrows( CommandFailed.class, () -> handler.createContext( requiredAnd( "--additional-config=" + additionalConf ) ) );

        // then
        assertThat( error.getMessage(), containsString( "does not exist" ) );
    }

    @Test
    void prioritiseConfigDirOverHomeDir() throws Exception
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
    void prioritiseAdditionalOverConfigDir() throws Exception
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
    void mustIgnorePageCacheConfigInConfigFile() throws Exception
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
    void mustIgnorePageCacheConfigInAdditionalConfigFile() throws Exception
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
    void mustRespectPageCacheConfigFromCommandLineArguments() throws Exception
    {
        // when
        OnlineBackupContextFactory builder = new OnlineBackupContextFactory( homeDir, configDir );
        OnlineBackupContext context = builder.createContext( requiredAnd( "--pagecache=42m" ) );

        // then
        assertThat( context.getConfig().get( pagecache_memory ), is( "42m" ) );
    }

    @Test
    void logsMustBePlacedInTargetBackupDirectory() throws Exception
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
    void metricsShouldBeDisabled() throws CommandFailed, IncorrectUsage
    {
        OnlineBackupContext context = new OnlineBackupContextFactory( homeDir, configDir ).createContext( requiredAnd() );

        Config config = context.getConfig();

        assertEquals( Optional.of( Settings.FALSE ), config.getRaw( "metrics.enabled" ) );
    }

    @Test
    void pageCacheWarmupShouldBeDisabled() throws CommandFailed, IncorrectUsage
    {
        OnlineBackupContext context = new OnlineBackupContextFactory( homeDir, configDir ).createContext( requiredAnd() );

        Config config = context.getConfig();

        assertFalse( config.get( pagecache_warmup_enabled ) );
    }

    @Test
    void ipv6CanBeProcessed() throws CommandFailed, IncorrectUsage
    {
        // given
        OnlineBackupContextFactory builder = new OnlineBackupContextFactory( homeDir, configDir );

        // when
        OnlineBackupContext context = builder.createContext( requiredAnd( "--from=[fd00:ce10::2]:6362" ) );

        // then
        assertEquals( "fd00:ce10::2", context.getRequiredArguments().getAddress().getHostname() );
        assertEquals( 6362, context.getRequiredArguments().getAddress().getPort() );
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
