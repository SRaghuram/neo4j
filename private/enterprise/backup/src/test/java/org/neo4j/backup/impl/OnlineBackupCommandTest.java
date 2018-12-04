/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Collections;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.admin.ParameterisedOutsideWorld;
import org.neo4j.commandline.admin.Usage;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.OptionalHostnamePort;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.text.StringContainsInOrder.stringContainsInOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class OnlineBackupCommandTest
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    private BackupStrategyCoordinatorFactory backupStrategyCoordinatorFactory = mock( BackupStrategyCoordinatorFactory.class );
    private BackupStrategyCoordinator backupStrategyCoordinator = mock( BackupStrategyCoordinator.class );

    private ByteArrayOutputStream baosOut = new ByteArrayOutputStream();
    private ByteArrayOutputStream baosErr = new ByteArrayOutputStream();
    private PrintStream stdout = new PrintStream( baosOut );
    private PrintStream stderr = new PrintStream( baosErr );
    private OutsideWorld outsideWorld;

    // Parameters and helpers
    private final Config config = Config.defaults();
    private OnlineBackupRequiredArguments requiredArguments;
    private final ConsistencyFlags consistencyFlags = new ConsistencyFlags( true, true, true, true );

    private Path backupDirectory;
    private Path reportDirectory;
    private BackupSupportingClassesFactory backupSupportingClassesFactory =
            mock( BackupSupportingClassesFactory.class );

    private final OptionalHostnamePort address = new OptionalHostnamePort( "hostname", 12, 34 );
    private final String backupName = "backup name";
    private final boolean fallbackToFull = true;
    private final boolean doConsistencyCheck = true;
    private final long timeout = 1000;

    private OnlineBackupCommand subject;

    @BeforeEach
    void setup()
    {
        outsideWorld = new ParameterisedOutsideWorld( System.console(), stdout, stderr, System.in, fs );
        backupDirectory = testDirectory.directory( "backupDirectory" ).toPath();
        reportDirectory = testDirectory.directory( "reportDirectory/" ).toPath();
        BackupSupportingClasses backupSupportingClasses =
                new BackupSupportingClasses( mock( BackupDelegator.class ), mock( PageCache.class ), Collections.emptyList() );
        when( backupSupportingClassesFactory.createSupportingClasses( any() ) ).thenReturn( backupSupportingClasses );

        requiredArguments =
                new OnlineBackupRequiredArguments( address, null, backupDirectory, backupName, fallbackToFull,
                        doConsistencyCheck, timeout, reportDirectory );
        OnlineBackupContext onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags );

        when( backupStrategyCoordinatorFactory.backupStrategyCoordinator( any(), any(), any() ) ).thenReturn( backupStrategyCoordinator );

        subject = newOnlineBackupCommand( outsideWorld, onlineBackupContext, backupSupportingClassesFactory, backupStrategyCoordinatorFactory );
    }

    @Test
    void nonExistingBackupDirectoryRaisesException() throws CommandFailed, IncorrectUsage, IOException
    {
        // given backup directory is not a directory
        fs.deleteRecursively( backupDirectory.toFile() );
        fs.create( backupDirectory.toFile() ).close();

        // then
        CommandFailed error = assertThrows( CommandFailed.class, this::execute );
        assertThat( error.getMessage(), stringContainsInOrder( asList( "Directory '", "backupDirectory' does not exist." ) ) );
    }

    @Test
    void nonExistingReportDirectoryRaisesException() throws CommandFailed, IncorrectUsage, IOException
    {
        // given report directory is not a directory
        fs.deleteRecursively( reportDirectory.toFile() );
        fs.create( reportDirectory.toFile() ).close();

        // then
        CommandFailed error = assertThrows( CommandFailed.class, this::execute );
        assertThat( error.getMessage(), stringContainsInOrder( asList( "Directory '", "reportDirectory' does not exist." ) ) );
    }

    @Test
    void shouldPrintNiceHelp()
    {
        Usage usage = new Usage( "neo4j-admin", mock( CommandLocator.class ) );
        usage.printUsageForCommand( new OnlineBackupCommandProvider(), stdout::println );

        assertEquals( format( "usage: neo4j-admin backup --backup-dir=<backup-path> --name=<graph.db-backup>%n" +
                              "                          [--from=<address>] [--database=<graph.db>]%n" +
                              "                          [--fallback-to-full[=<true|false>]]%n" +
                              "                          [--timeout=<timeout>] [--pagecache=<8m>]%n" +
                              "                          [--check-consistency[=<true|false>]]%n" +
                              "                          [--cc-report-dir=<directory>]%n" +
                              "                          [--additional-config=<config-file-path>]%n" +
                              "                          [--cc-graph[=<true|false>]]%n" +
                              "                          [--cc-indexes[=<true|false>]]%n" +
                              "                          [--cc-label-scan-store[=<true|false>]]%n" +
                              "                          [--cc-property-owners[=<true|false>]]%n" +
                              "%n" +
                              "environment variables:%n" +
                              "    NEO4J_CONF    Path to directory which contains neo4j.conf.%n" +
                              "    NEO4J_DEBUG   Set to anything to enable debug output.%n" +
                              "    NEO4J_HOME    Neo4j home directory.%n" +
                              "    HEAP_SIZE     Set JVM maximum heap size during command execution.%n" +
                              "                  Takes a number and a unit, for example 512m.%n" +
                              "%n" +
                              "Perform an online backup from a running Neo4j enterprise server. Neo4j's backup%n" +
                              "service must have been configured on the server beforehand.%n" +
                              "%n" +
                              "All consistency checks except 'cc-graph' can be quite expensive so it may be%n" +
                              "useful to turn them off for very large databases. Increasing the heap size can%n" +
                              "also be a good idea. See 'neo4j-admin help' for details.%n" +
                              "%n" +
                              "For more information see:%n" +
                              "https://neo4j.com/docs/operations-manual/current/backup/%n" +
                              "%n" +
                              "options:%n" +
                              "  --backup-dir=<backup-path>               Directory to place backup in.%n" +
                              "  --name=<graph.db-backup>                 Name of backup. If a backup with this%n" +
                              "                                           name already exists an incremental%n" +
                              "                                           backup will be attempted.%n" +
                              "  --from=<address>                         Host and port of Neo4j.%n" +
                              "                                           [default:localhost:6362]%n" +
                              "  --database=<graph.db>                    Name of the remote database to%n" +
                              "                                           backup. [default:null]%n" +
                              "  --fallback-to-full=<true|false>          If an incremental backup fails backup%n" +
                              "                                           will move the old backup to%n" +
                              "                                           <name>.err.<N> and fallback to a full%n" +
                              "                                           backup instead. [default:true]%n" +
                              "  --timeout=<timeout>                      Timeout in the form <time>[ms|s|m|h],%n" +
                              "                                           where the default unit is seconds.%n" +
                              "                                           [default:20m]%n" +
                              "  --pagecache=<8m>                         The size of the page cache to use for%n" +
                              "                                           the backup process. [default:8m]%n" +
                              "  --check-consistency=<true|false>         If a consistency check should be%n" +
                              "                                           made. [default:true]%n" +
                              "  --cc-report-dir=<directory>              Directory where consistency report%n" +
                              "                                           will be written. [default:.]%n" +
                              "  --additional-config=<config-file-path>   Configuration file to supply%n" +
                              "                                           additional configuration in. This%n" +
                              "                                           argument is DEPRECATED. [default:]%n" +
                              "  --cc-graph=<true|false>                  Perform consistency checks between%n" +
                              "                                           nodes, relationships, properties,%n" +
                              "                                           types and tokens. [default:true]%n" +
                              "  --cc-indexes=<true|false>                Perform consistency checks on%n" +
                              "                                           indexes. [default:true]%n" +
                              "  --cc-label-scan-store=<true|false>       Perform consistency checks on the%n" +
                              "                                           label scan store. [default:true]%n" +
                              "  --cc-property-owners=<true|false>        Perform additional consistency checks%n" +
                              "                                           on property ownership. This check is%n" +
                              "                                           *very* expensive in time and memory.%n" +
                              "                                           [default:false]%n" ), baosOut.toString() );
    }

    @Test
    void protocolSupportingDatabaseNameWorksFine() throws CommandFailed, IncorrectUsage
    {
        // given
        requiredArguments = new OnlineBackupRequiredArguments( address, "graph.db", backupDirectory, backupName,
                fallbackToFull, doConsistencyCheck, timeout, reportDirectory );
        OnlineBackupContext onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags );
        subject = newOnlineBackupCommand( outsideWorld, onlineBackupContext, backupSupportingClassesFactory, backupStrategyCoordinatorFactory );

        // when
        execute();

        // then: does not throw
    }

    private static OnlineBackupCommand newOnlineBackupCommand( OutsideWorld outsideWorld, OnlineBackupContext onlineBackupContext,
            BackupSupportingClassesFactory backupSupportingClassesFactory, BackupStrategyCoordinatorFactory backupStrategyCoordinatorFactory )
    {
        OnlineBackupContextFactory contextBuilder = mock( OnlineBackupContextFactory.class );
        try
        {
            when( contextBuilder.createContext( any() ) ).thenReturn( onlineBackupContext );
        }
        catch ( IncorrectUsage | CommandFailed e )
        {
            throw new RuntimeException( "Shouldn't happen", e );
        }

        return new OnlineBackupCommand( outsideWorld, contextBuilder, backupSupportingClassesFactory, backupStrategyCoordinatorFactory );
    }

    private void execute() throws IncorrectUsage, CommandFailed
    {
        String[] implementationDoesNotUseArguments = new String[0];
        subject.execute( implementationDoesNotUseArguments );
    }
}
