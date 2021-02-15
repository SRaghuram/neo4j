/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.migration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.helpers.Args;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.extension.DatabaseExtensions;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.DatabaseExtensionContext;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.storemigration.DatabaseMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.LogTailInformation;
import org.neo4j.kernel.impl.transaction.state.DefaultIndexProviderMap;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.LogConfig;
import org.neo4j.logging.log4j.Neo4jLoggerContext;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.time.Stopwatch;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.store_internal_log_path;
import static org.neo4j.kernel.extension.ExtensionFailureStrategies.ignore;
import static org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory.createPageCache;
import static org.neo4j.monitoring.PanicEventGenerator.NO_OP;

/**
 * Stand alone tool for migrating/upgrading a neo4j database from one version to the next.
 */
public final class StoreMigration
{
    private static final String HELP_FLAG = "help";

    private StoreMigration()
    {
    }

    public static void main( String[] args ) throws Exception
    {
        Args arguments = Args.withFlags( HELP_FLAG ).parse( args );
        if ( arguments.getBoolean( HELP_FLAG, false ) || args.length == 0 )
        {
            printUsageAndExit();
        }
        Path storeDir = parseDir( arguments );

        LogProvider userLogProvider = new Log4jLogProvider( System.out );
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            StoreMigration.run( fileSystem, storeDir, getMigrationConfig( storeDir ), userLogProvider );
        }
    }

    private static Config getMigrationConfig( Path workingDirectory )
    {
        return Config.newBuilder()
                .set( GraphDatabaseSettings.allow_upgrade, true )
                .set( logs_directory, workingDirectory.toAbsolutePath() )
                .build();
    }

    public static void run( final FileSystemAbstraction fs, final Path storeDirectory, Config config, LogProvider userLogProvider ) throws Exception
    {
        Neo4jLoggerContext ctx = LogConfig.createBuilder( fs, config.get( store_internal_log_path ), Level.INFO ).build();

        SimpleLogService logService = new SimpleLogService( userLogProvider, new Log4jLogProvider( ctx ) );

        LifeSupport life = new LifeSupport();

        life.add( logService );

        // Add participants from kernel extensions...
        Log log = userLogProvider.getLog( StoreMigration.class );
        JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        var pageCacheTracer = PageCacheTracer.NULL;
        var memoryTracker = EmptyMemoryTracker.INSTANCE;
        try ( PageCache pageCache = createPageCache( fs, config, jobScheduler, pageCacheTracer ) )
        {
            Dependencies deps = new Dependencies();
            Monitors monitors = new Monitors();
            deps.satisfyDependencies( fs, config, pageCache, logService, monitors,
                    RecoveryCleanupWorkCollector.immediate(), pageCacheTracer );

            DatabaseLayout databaseLayout = DatabaseLayout.ofFlat( storeDirectory );
            DatabaseExtensionContext extensionContext = new DatabaseExtensionContext( databaseLayout, DbmsInfo.UNKNOWN, deps );
            Iterable<ExtensionFactory<?>> extensionFactories = GraphDatabaseDependencies.newDependencies().extensions();
            DatabaseExtensions databaseExtensions = life.add( new DatabaseExtensions( extensionContext, extensionFactories, deps, ignore() ) );
            StorageEngineFactory storageEngineFactory = StorageEngineFactory.selectStorageEngine();

            final LogFiles logFiles = LogFilesBuilder.activeFilesBuilder( databaseLayout, fs, pageCache )
                    .withConfig( config )
                    .withMemoryTracker( memoryTracker )
                    .withStoreId( StoreId.UNKNOWN )
                    .build();

            deps.satisfyDependency( life.add( new DefaultIndexProviderMap( databaseExtensions, config ) ) );

            // Add the kernel store migrator
            life.start();

            Stopwatch startTime = Stopwatch.start();
            DatabaseMigrator migrator = new DatabaseMigrator( fs, config, logService, deps, pageCache,  jobScheduler, databaseLayout,
                    storageEngineFactory, pageCacheTracer, memoryTracker, new DatabaseHealth( NO_OP, log ) );
            migrator.migrate( true );

            // Append checkpoint so the last log entry will have the latest version
            appendCheckpoint( logFiles );

            log.info( format( "Migration completed in %d s%n", startTime.elapsed().getSeconds() ) );
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

    private static void appendCheckpoint( LogFiles logFiles ) throws IOException
    {
        try ( Lifespan lifespan = new Lifespan( logFiles ) )
        {
            var checkpointAppender = logFiles.getCheckpointFile().getCheckpointAppender();
            LogTailInformation tailInformation = logFiles.getTailInformation();
            LogPosition logPosition = tailInformation.lastCheckPoint.getTransactionLogPosition();
            checkpointAppender.checkPoint( LogCheckPointEvent.NULL, logPosition, Instant.now(), "Store migration tool." );
        }
    }

    private static Path parseDir( Args args )
    {
        if ( args.orphans().size() != 1 )
        {
            System.out.println( "Error: too much arguments provided." );
            printUsageAndExit();
        }
        Path dir = Path.of( args.orphans().get( 0 ) );
        if ( !Files.isDirectory( dir ) )
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
