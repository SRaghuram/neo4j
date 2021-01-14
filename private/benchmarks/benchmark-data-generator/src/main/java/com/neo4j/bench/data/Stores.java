/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.data;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Neo4jStore;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.profiling.FullBenchmarkName;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.util.JsonUtil;
import com.neo4j.dbms.api.EnterpriseDatabaseManagementServiceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.concurrent.NotThreadSafe;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.util.VisibleForTesting;

import static com.neo4j.bench.common.util.BenchmarkUtil.bytes;
import static com.neo4j.bench.common.util.BenchmarkUtil.bytesToString;
import static com.neo4j.bench.common.util.BenchmarkUtil.durationToString;
import static com.neo4j.bench.common.util.BenchmarkUtil.tryMkDir;
import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASES_ROOT_DIR_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATA_DIR_NAME;

/**
 * There can be only one store set {@link StoreUsage#inProgress}. Also {@link StoreUsage#save()} may override other threads (or VMs) changes if called
 * concurrently.
 */
@NotThreadSafe
public class Stores
{
    private static final Logger LOG = LoggerFactory.getLogger( Stores.class );

    private static final String DB_DIR_NAME = "graph.db";
    @VisibleForTesting
    static final String CONFIG_FILENAME = "data_gen_config.json";
    // Used by benchmarks that do not need a database
    // Definitely a hack that needs to be removed later, but that requires rewriting more of this class
    private static final String NULL_STORE_DIR_NAME = "no_database_lives_in_this_database_directory";

    private final Path storesDir;

    public Stores( Path storesDir )
    {
        this.storesDir = storesDir;
    }

    public Neo4jConfig neo4jConfigFor( BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        BenchmarkGroupDirectory benchmarkGroupDirectory = BenchmarkGroupDirectory.findOrFailAt( storesDir, benchmarkGroup );
        BenchmarkDirectory benchmarkDirectory = benchmarkGroupDirectory.findOrFail( benchmark );
        ForkDirectory forkDirectory = benchmarkDirectory.measurementForks().stream()
                                                        .findFirst()
                                                        .orElseThrow( () -> new RuntimeException( format( "No measurement forks found for '%s' in : %s",
                                                                                                          benchmark.name(),
                                                                                                          benchmarkDirectory.toAbsolutePath() ) ) );
        return Neo4jConfigBuilder.fromFile( forkDirectory.findOrFail( "neo4j.conf" ) ).build();
    }

    public StoreAndConfig prepareDb(
            DataGeneratorConfig config,
            BenchmarkGroup group,
            Benchmark benchmark,
            Augmenterizer augmenterizer,
            Path neo4jConfigFile,
            int threads )
    {
        deleteFailedStores();
        Path store = getOrGenerate( config, group, benchmark, augmenterizer, neo4jConfigFile, threads );
        StoreAndConfig storeAndConfig = new StoreAndConfig( storesDir, store, neo4jConfigFile, config.isReusable() );
        loadStoreUsage().setInProgress( store, group, benchmark );
        return storeAndConfig;
    }

    private Path getOrGenerate( DataGeneratorConfig config,
                                BenchmarkGroup group,
                                Benchmark benchmark,
                                Augmenterizer augmenterizer,
                                Path neo4jConfigFile,
                                int threads )
    {
        Optional<Path> maybeSecondaryCopy = findSecondaryCopy( config );

        if ( config.isReusable() && maybeSecondaryCopy.isPresent() )
        {
            // We've first cleaned up all failed copies, so we're sure that this copy must be reusable
            return maybeSecondaryCopy.get();
        }
        else
        {
            Path primaryCopy = getOrCreatePrimaryCopy( config, augmenterizer, neo4jConfigFile, threads );
            return getCopyOf( group, benchmark, primaryCopy, neo4jConfigFile );
        }
    }

    private Path getOrCreatePrimaryCopy( DataGeneratorConfig config,
                                         Augmenterizer augmenterizer,
                                         Path neo4jConfigFile,
                                         int threads )
    {

        return findPrimaryCopy( config )
                .orElseGet( () -> generateDb( config, augmenterizer, neo4jConfigFile, threads ) );
    }

    private Path generateDb(
            DataGeneratorConfig config,
            Augmenterizer augmenterizer,
            Path neo4jConfigFile,
            int threads )
    {
        Path topLevelStoreDir = randomTopLevelStoreDir();
        tryMkDir( topLevelStoreDir );

        LOG.debug( "Generating store in: " + topLevelStoreDir.toAbsolutePath() );
        LOG.debug( config.toString() );

        // will create an empty database directory under top level
        new EnterpriseDatabaseManagementServiceBuilder( topLevelStoreDir )
                .setConfigRaw( config.neo4jConfig().toMap() )
                .build()
                .shutdown();
        try
        {
            new DataGenerator( config ).generate( Neo4jStore.createFrom( topLevelStoreDir ), neo4jConfigFile );
        }
        catch ( Exception e )
        {
            LOG.debug( "Deleting failed store: " + topLevelStoreDir.toAbsolutePath() );
            deleteStore( topLevelStoreDir );
            throw new RuntimeException( "Error creating store at: " + topLevelStoreDir.toAbsolutePath(), e );
        }
        StoreAndConfig storeAndConfig = new StoreAndConfig( storesDir, topLevelStoreDir, neo4jConfigFile, config.isReusable() );

        LOG.debug( "Executing store augmentation step..." );
        Instant augmentStart = Instant.now();
        augmenterizer.augment( threads, storeAndConfig );
        Duration augmentDuration = Duration.between( augmentStart, Instant.now() );
        LOG.debug( "Store augmentation step took: " + durationToString( augmentDuration ) );

        loadStoreUsage().registerPrimary( topLevelStoreDir );
        config.serialize( topLevelStoreDir.resolve( CONFIG_FILENAME ) );
        return topLevelStoreDir;
    }

    private Path getCopyOf(
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            Path topLevelDir,
            Path config )
    {
        LOG.debug( "Reusing copy of store...\n" +
                   "  > Benchmark group: " + benchmarkGroup.name() + "\n" +
                   "  > Benchmark:       " + benchmark.name() + "\n" +
                   "  > Original store:  " + topLevelDir.toAbsolutePath() + "\n" +
                   "  > Config:          " + config.toAbsolutePath() );
        Path newTopLevelDir = getCopyOf( topLevelDir );
        LOG.debug( "Copied: " + bytesToString( BenchmarkUtil.bytes( topLevelDir ) ) + "\n" +
                   "  > Store copy:      " + newTopLevelDir.toAbsolutePath() );
        loadStoreUsage().registerSecondary( topLevelDir, newTopLevelDir );
        return newTopLevelDir;
    }

    private Path getCopyOf( Path from )
    {
        Path to = randomTopLevelStoreDir();
        try
        {
            CopyDirVisitor visitor = new CopyDirVisitor( from, to );
            Files.walkFileTree( from, visitor );
            visitor.awaitCompletion();
            return to;
        }
        catch ( Exception e )
        {
            IOException ioe;
            if ( e instanceof IOException )
            {
                ioe = (IOException) e;
            }
            else
            {
                ioe = new IOException( e );
            }
            throw new UncheckedIOException( format( "Error copying DB from %s to %s", from, to ), ioe );
        }
    }

    private Path randomTopLevelStoreDir()
    {
        return storesDir.resolve( UUID.randomUUID().toString() );
    }

    public void deleteFailedStores()
    {
        StoreUsage storeUsage = loadStoreUsage();
        storeUsage.getInProgress( storesDir )
                  .ifPresent( store ->
                              {
                                  deleteStore( store );
                                  storeUsage.clearInProgress( store );
                              } );
    }

    private static void deleteStore( Path store )
    {
        try
        {
            LOG.debug( format( "Deleting store [%s] at: %s", bytesToString( bytes( store ) ), store ) );
            FileUtils.deleteDirectory( store );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Could not delete store: " + store, e );
        }
    }

    public String details()
    {
        StringBuilder sb = new StringBuilder()
                .append( "-----------------------------------------------------------------------------------------\n" )
                .append( "------------------------------ STORE DIRECTORY DETAILS ----------------------------------\n" )
                .append( "-----------------------------------------------------------------------------------------\n" )
                .append( format( "\t%1$-20s %2$s\n", bytesToString( BenchmarkUtil.bytes( storesDir ) ),
                                 storesDir.toAbsolutePath() ) )
                .append( "---------------------------------------------------------------------\n" );
        StoreUsage storeUsage = loadStoreUsage();
        for ( Map.Entry<Path,Set<String>> entry : storeUsage.allStoreBenchmarkInfo( storesDir ).entrySet() )
        {
            Path topLevelDir = entry.getKey();
            Set<String> benchmarks = entry.getValue();
            sb.append( format( "\t%1$-20s %2$s\n", bytesToString( bytes( topLevelDir ) ), topLevelDir.toAbsolutePath() ) );
            for ( String benchmarkName : benchmarks )
            {
                sb.append( "\t\t" ).append( benchmarkName ).append( "\n" );
            }
        }
        return sb
                .append( "-----------------------------------------------------------------------------------------\n" )
                .toString();
    }

    private Optional<Path> findPrimaryCopy( DataGeneratorConfig config )
    {
        return findStoreMatchingConfig( config, true );
    }

    private Optional<Path> findSecondaryCopy( DataGeneratorConfig config )
    {
        return findStoreMatchingConfig( config, false );
    }

    private Optional<Path> findStoreMatchingConfig( DataGeneratorConfig config, boolean primary )
    {
        List<Path> topLevelDirs = findAllStoresMatchingConfig( config, primary );

        if ( topLevelDirs.isEmpty() )
        {
            return Optional.empty();
        }
        else if ( topLevelDirs.size() == 1 )
        {
            return Optional.of( topLevelDirs.get( 0 ) );
        }
        else
        {
            throw new IllegalStateException( format( "Found too many stores for config\n" +
                                                     "Stores: %s\n" +
                                                     "%s", topLevelDirs, config ) );
        }
    }

    // returns all stores with equal configs, where 'equal' means equal in all ways except
    //  (1) re-usability
    //  (2) that neither config has been augmented
    private List<Path> findAllStoresMatchingConfig( DataGeneratorConfig config, boolean isPrimary )
    {
        StoreUsage storeUsage = loadStoreUsage();
        return findAllTopLevelDirs().stream()
                                    .filter( topLevelDir -> storeMatchesConfig( config, topLevelDir ) )
                                    .filter( path -> isPrimary == storeUsage.isPrimary( path ) )
                                    .collect( Collectors.toList() );
    }

    private static boolean storeMatchesConfig( DataGeneratorConfig config, Path topLevelDir )
    {
        Path storeConfigFile = topLevelDir.resolve( CONFIG_FILENAME );
        return Files.exists( storeConfigFile ) &&
               DataGeneratorConfig.from( storeConfigFile ).equals( config );
    }

    private List<Path> findAllTopLevelDirs()
    {
        try ( Stream<Path> entries = Files.list( storesDir ) )
        {
            return entries.filter( Stores::isTopLevelDir ).collect( Collectors.toList() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public static boolean isTopLevelDir( Path topStoreLevelDir )
    {
        Path dbDir = topStoreLevelDir.resolve( Path.of( DEFAULT_DATA_DIR_NAME, DEFAULT_DATABASES_ROOT_DIR_NAME, DEFAULT_DATABASE_NAME ) );
        return
                (
                        // contains a graph.db directory
                        Files.exists( dbDir ) &&
                        // directory containing a real store
                        Files.exists( topStoreLevelDir.resolve( CONFIG_FILENAME ) )
                ) ||
                // place holder 'store' directory, used by all non-database benchmarks
                topStoreLevelDir.endsWith( NULL_STORE_DIR_NAME );
    }

    @VisibleForTesting
    StoreUsage loadStoreUsage()
    {
        return StoreUsage.loadOrCreateIfAbsent( storesDir );
    }

    public static class StoreAndConfig implements AutoCloseable
    {
        private final Path storesDir;
        private final Store store;
        private final Path config;
        private final boolean reusable;

        private StoreAndConfig( Path storesDir, Path topLevelStoreDir, Path config, boolean reusable )
        {
            this.storesDir = storesDir;
            this.store = Neo4jStore.createFrom( topLevelStoreDir );
            this.config = config;
            this.reusable = reusable;
        }

        public Store store()
        {
            return store;
        }

        public Path config()
        {
            return config;
        }

        @Override
        public void close()
        {
            if ( !reusable )
            {
                deleteStore( store.topLevelDirectory() );
            }
            StoreUsage.loadOrCreateIfAbsent( storesDir )
                      .clearInProgress( store.topLevelDirectory() );
        }
    }

    private static class CopyDirVisitor extends SimpleFileVisitor<Path>
    {
        private final ExecutorService executorService;
        private final List<Future<Void>> copyingProcesses;
        private final Path fromPath;
        private final Path toPath;

        private CopyDirVisitor( Path fromPath, Path toPath )
        {
            this.fromPath = fromPath;
            this.toPath = toPath;
            executorService = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );
            copyingProcesses = new ArrayList<>();
        }

        @Override
        public FileVisitResult preVisitDirectory( Path dir, BasicFileAttributes attrs ) throws IOException
        {
            Path targetPath = toPath.resolve( fromPath.relativize( dir ) );
            if ( !Files.exists( targetPath ) )
            {
                Files.createDirectory( targetPath );
            }
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile( Path file, BasicFileAttributes attrs )
        {
            copyingProcesses.add( executorService.submit( () ->
                                                          {
                                                              Files.copy( file, toPath.resolve( fromPath.relativize( file ) ) );
                                                              return null;
                                                          } ) );
            return FileVisitResult.CONTINUE;
        }

        private void awaitCompletion() throws Exception
        {
            for ( Future<Void> copyingProcess : copyingProcesses )
            {
                copyingProcess.get();
            }
            executorService.shutdown();
        }

    }

    static class StoreUsage
    {
        private static final String STORE_USAGE_JSON = "store-usage.json";

        private static StoreUsage loadOrCreateIfAbsent( Path dir )
        {
            Path jsonPath = dir.resolve( STORE_USAGE_JSON );
            if ( !Files.exists( jsonPath ) )
            {
                try
                {
                    Files.createFile( jsonPath );
                }
                catch ( IOException e )
                {
                    throw new UncheckedIOException( "Failed to create: " + jsonPath.toAbsolutePath(), e );
                }
                StoreUsage storeUsage = new StoreUsage( jsonPath );
                storeUsage.save();
                return storeUsage;
            }
            else
            {
                return JsonUtil.deserializeJson( jsonPath, StoreUsage.class );
            }
        }

        private Path jsonPath;
        private Map<String,Set<String>> primaryToBenchmarks = new HashMap<>();
        private Map<String,String> secondaryToPrimary = new HashMap<>();
        private String inProgress;

        /**
         * WARNING: Never call this explicitly. No-params constructor is only used for JSON (de)serialization.
         */
        private StoreUsage()
        {
            this( null );
        }

        private StoreUsage( Path jsonPath )
        {
            this.jsonPath = jsonPath;
        }

        /**
         * 'Registering' a benchmark with a store means that the store was used by that benchmark.
         *
         * @throws IllegalStateException if benchmark had already been registered previously.
         */
        @VisibleForTesting
        void registerPrimary( Path store )
        {
            String storeName = getStoreName( store );
            if ( primaryToBenchmarks.containsKey( storeName ) )
            {
                throw new IllegalStateException( format( "Store '%s' was already registered", storeName ) );
            }
            primaryToBenchmarks.put( storeName, new HashSet<>() );
            save();
        }

        @VisibleForTesting
        public void registerSecondary( Path primaryStore, Path secondaryStore )
        {
            String primaryStoreName = getStoreName( primaryStore );
            String secondaryStoreName = getStoreName( secondaryStore );
            if ( !primaryToBenchmarks.containsKey( primaryStoreName ) )
            {
                throw new IllegalStateException( format( "Store '%s' has unknown primary store '%s'", secondaryStoreName, primaryStoreName ) );
            }
            if ( secondaryToPrimary.containsKey( secondaryStoreName ) )
            {
                throw new IllegalStateException( format( "Store '%s' was already registered to primary store '%s'", secondaryStoreName,
                                                         secondaryToPrimary.get( secondaryStoreName ) ) );
            }
            secondaryToPrimary.put( secondaryStoreName, primaryStoreName );
            save();
        }

        private boolean isPrimary( Path store )
        {
            return primaryToBenchmarks.containsKey( getStoreName( store ) );
        }

        void setInProgress( Path store, BenchmarkGroup group, Benchmark benchmark )
        {
            Objects.requireNonNull( store );
            if ( inProgress != null )
            {
                throw new IllegalStateException( format( "Store '%s' was in progress", inProgress ) );
            }
            String storeName = getStoreName( store );
            trackHistory( storeName, group, benchmark );
            inProgress = storeName;
            save();
        }

        private void trackHistory( String secondaryStoreName, BenchmarkGroup group, Benchmark benchmark )
        {
            String benchmarkName = FullBenchmarkName.from( group, benchmark ).name();
            if ( !secondaryToPrimary.containsKey( secondaryStoreName ) )
            {
                throw new IllegalStateException( format( "Benchmark '%s' has unknown primary store '%s'", benchmarkName, secondaryStoreName ) );
            }
            String primaryStoreName = secondaryToPrimary.get( secondaryStoreName );
            Set<String> benchmarks = primaryToBenchmarks.get( primaryStoreName );
            benchmarks.add( benchmarkName );
        }

        private Optional<Path> getInProgress( Path storesDir )
        {
            return Optional.ofNullable( inProgress ).map( storesDir::resolve );
        }

        void clearInProgress( Path store )
        {
            String storeName = getStoreName( store );
            if ( !storeName.equals( inProgress ) )
            {
                throw new IllegalStateException( format( "Store '%s' was not in progress (currently in progress %s)", storeName, inProgress ) );
            }
            inProgress = null;
            save();
        }

        private static String getStoreName( Path store )
        {
            return store.getFileName().toString();
        }

        Map<Path,Set<String>> allStoreBenchmarkInfo( Path storesDir )
        {
            return primaryToBenchmarks.entrySet()
                                      .stream()
                                      .collect( Collectors.toMap( entry -> storesDir.resolve( entry.getKey() ), Map.Entry::getValue ) );
        }

        private void save()
        {
            JsonUtil.serializeJson( jsonPath, this );
        }
    }
}
