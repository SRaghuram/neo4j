/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.migration;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.extension.DatabaseKernelExtensions;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.extension.context.DatabaseExtensionContext;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.storemigration.DatabaseMigratorImpl;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.transaction.log.FlushablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.state.DefaultIndexProviderMap;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.StoreLogService;
import org.neo4j.scheduler.JobScheduler;

import static java.lang.String.format;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_internal_log_path;
import static org.neo4j.kernel.extension.KernelExtensionFailureStrategies.ignore;
import static org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory.createPageCache;

/**
 * Stand alone tool for migrating/upgrading a neo4j database from one version to the next.
 *
 * @see StoreMigrator
 */
//: TODO introduce abstract tool class as soon as we will have several tools in tools module
public class StoreMigration
{
    private static final String HELP_FLAG = "help";

    public static void main( String[] args ) throws Exception
    {
        Args arguments = Args.withFlags( HELP_FLAG ).parse( args );
        if ( arguments.getBoolean( HELP_FLAG, false ) || args.length == 0 )
        {
            printUsageAndExit();
        }
        File storeDir = parseDir( arguments );

        FormattedLogProvider userLogProvider = FormattedLogProvider.toOutputStream( System.out );
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            new StoreMigration().run( fileSystem, storeDir, getMigrationConfig(), userLogProvider );
        }
    }

    private static Config getMigrationConfig()
    {
        return Config.defaults( GraphDatabaseSettings.allow_upgrade, Settings.TRUE );
    }

    public static void run( final FileSystemAbstraction fs, final File storeDirectory, Config config, LogProvider userLogProvider ) throws Exception
    {
        StoreLogService logService = StoreLogService.withUserLogProvider( userLogProvider )
                .withInternalLog( config.get( store_internal_log_path ) ).build( fs );

        LifeSupport life = new LifeSupport();

        life.add( logService );

        // Add participants from kernel extensions...
        Log log = userLogProvider.getLog( StoreMigration.class );
        JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        try ( PageCache pageCache = createPageCache( fs, config, jobScheduler ) )
        {
            Dependencies deps = new Dependencies();
            Monitors monitors = new Monitors();
            deps.satisfyDependencies( fs, config, pageCache, logService, monitors,
                    RecoveryCleanupWorkCollector.immediate() );

            DatabaseLayout databaseLayout = DatabaseLayout.of( storeDirectory );
            DatabaseExtensionContext extensionContext = new DatabaseExtensionContext( databaseLayout, DatabaseInfo.UNKNOWN, deps );
            Iterable<KernelExtensionFactory<?>> kernelExtensionFactories = GraphDatabaseDependencies.newDependencies().kernelExtensions();
            DatabaseKernelExtensions kernelExtensions = life.add( new DatabaseKernelExtensions( extensionContext, kernelExtensionFactories, deps, ignore() ) );

            final LogFiles logFiles = LogFilesBuilder.activeFilesBuilder( databaseLayout, fs, pageCache )
                    .withConfig( config ).build();
            LogTailScanner tailScanner = new LogTailScanner( logFiles, new VersionAwareLogEntryReader<>(), monitors );

            DefaultIndexProviderMap indexProviderMap = life.add( new DefaultIndexProviderMap( kernelExtensions, config ) );

            // Add the kernel store migrator
            life.start();

            long startTime = System.currentTimeMillis();
            DatabaseMigratorImpl migrator = new DatabaseMigratorImpl( fs, config, logService,
                    indexProviderMap,
                    pageCache, tailScanner, jobScheduler, databaseLayout );
            migrator.migrate();

            // Append checkpoint so the last log entry will have the latest version
            appendCheckpoint( logFiles, tailScanner );

            long duration = System.currentTimeMillis() - startTime;
            log.info( format( "Migration completed in %d s%n", duration / 1000 ) );
        }
        catch ( Exception e )
        {
            throw new StoreUpgrader.UnableToUpgradeException( "Failure during upgrade", e );
        }
        finally
        {
            life.shutdown();
            jobScheduler.close();
        }
    }

    private static void appendCheckpoint( LogFiles logFiles, LogTailScanner tailScanner ) throws IOException
    {
        try ( Lifespan lifespan = new Lifespan( logFiles ) )
        {
            FlushablePositionAwareChannel writer = logFiles.getLogFile().getWriter();
            TransactionLogWriter transactionLogWriter = new TransactionLogWriter( new LogEntryWriter( writer ) );
            transactionLogWriter.checkPoint( tailScanner.getTailInformation().lastCheckPoint.getLogPosition() );
            writer.prepareForFlush().flush();
        }
    }

    private static File parseDir( Args args )
    {
        if ( args.orphans().size() != 1 )
        {
            System.out.println( "Error: too much arguments provided." );
            printUsageAndExit();
        }
        File dir = new File( args.orphans().get( 0 ) );
        if ( !dir.isDirectory() )
        {
            System.out.println( "Invalid directory: '" + dir + "'" );
            printUsageAndExit();
        }
        return dir;
    }

    private static void printUsageAndExit()
    {
        System.out.println( "Store migration tool performs migration of a store in specified location to latest " +
                            "supported store version." );
        System.out.println();
        System.out.println( "Options:" );
        System.out.println( "-help    print this help message" );
        System.out.println();
        System.out.println( "Usage:" );
        System.out.println( "./storeMigration [option] <store directory>" );
        System.exit( 1 );
    }
}
