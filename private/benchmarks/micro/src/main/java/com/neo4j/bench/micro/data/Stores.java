/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Neo4jStore;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.profiling.FullBenchmarkName;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.micro.benchmarks.Kaboom;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.util.JsonUtil;
import com.neo4j.dbms.api.EnterpriseDatabaseManagementServiceBuilder;

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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.neo4j.io.fs.FileUtils;

import static com.neo4j.bench.common.util.BenchmarkUtil.bytesToString;
import static com.neo4j.bench.common.util.BenchmarkUtil.durationToString;
import static com.neo4j.bench.common.util.BenchmarkUtil.tryMkDir;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASES_ROOT_DIR_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATA_DIR_NAME;

public class Stores
{
    private static final String DB_DIR_NAME = "graph.db";
    private static final String CONFIG_FILENAME = "data_gen_config.json";
    private static final String TEMP_STORE_COPY_MARKER_FILENAME = "this_is_a_temporary_store.tmp";
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
        List<Path> topLevelDirs = findAllStoresMatchingConfig( config, storesDir );

        if ( topLevelDirs.isEmpty() )
        {
            StoreAndConfig initialStoreAndConfig = generateDb(
                    config,
                    augmenterizer,
                    group,
                    benchmark,
                    neo4jConfigFile,
                    threads );
            if ( config.isReusable() )
            {
                return initialStoreAndConfig;
            }
            else
            {
                return getCopyOf( group, benchmark, initialStoreAndConfig );
            }
        }
        else if ( topLevelDirs.size() == 1 )
        {
            Path topLevelDir = topLevelDirs.get( 0 );

            // if configs are identical in all ways except re-usability, make persisted config not-reusable
            // saves time (only generate once) & saves space (only one permanent store per equivalent config)
            // reminder: re-usable means 'mv store/ temp_copy/' does not need to be done for every run
            DataGeneratorConfig existingStoreConfig = DataGeneratorConfig.from( topLevelDir.resolve( CONFIG_FILENAME ) );
            if ( existingStoreConfig.isReusable() && !config.isReusable() )
            {
                config = DataGeneratorConfigBuilder
                        .from( config )
                        .isReusableStore( false )
                        .build();
                config.serialize( topLevelDir.resolve( CONFIG_FILENAME ) );
            }

            if ( config.isReusable() )
            {
                System.out.println( "Reusing store...\n" +
                                    "  > Benchmark group: " + group.name() + "\n" +
                                    "  > Benchmark:       " + benchmark.name() + "\n" +
                                    "  > Store:           " + topLevelDir.toAbsolutePath() + "\n" +
                                    "  > Config:          " + neo4jConfigFile.toAbsolutePath() );
                return new StoreAndConfig( topLevelDir, neo4jConfigFile );
            }
            else
            {
                return getCopyOf( group, benchmark, new StoreAndConfig( topLevelDir, neo4jConfigFile ) );
            }
        }
        else
        {
            throw new IllegalStateException( format( "Found too many stores for config\n" +
                                                     "Stores: %s\n" +
                                                     "%s", topLevelDirs, config ) );
        }
    }

    private StoreAndConfig generateDb(
            DataGeneratorConfig config,
            Augmenterizer augmenterizer,
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            Path neo4jConfigFile,
            int threads )
    {
        Path topLevelStoreDir = randomTopLevelStoreDir();
        tryMkDir( topLevelStoreDir );

        System.out.println( "Generating store in: " + topLevelStoreDir.toAbsolutePath() );
        System.out.println( config );

        // will create an empty database directory under top level
        new EnterpriseDatabaseManagementServiceBuilder( topLevelStoreDir )
                .setConfigRaw( config.neo4jConfig().toMap() )
                .build()
                .shutdown();

        try
        {
            new DataGenerator( config ).generate( Neo4jStore.createFrom( topLevelStoreDir ), neo4jConfigFile );
            String storeName = topLevelStoreDir.getFileName().toString();
            StoreUsage.loadOrCreateIfAbsent( storesDir )
                      .register( storeName, benchmarkGroup, benchmark );
        }
        catch ( Exception e )
        {
            try
            {
                System.out.println( "Deleting failed store: " + topLevelStoreDir.toFile().getAbsolutePath() );
                FileUtils.deleteRecursively( topLevelStoreDir.toFile() );
            }
            catch ( IOException ioe )
            {
                throw new UncheckedIOException( "Error deleting failed store: " + topLevelStoreDir.toFile().getAbsolutePath(), ioe );
            }
            throw new Kaboom( "Error creating store at: " + topLevelStoreDir.toFile().getAbsolutePath(), e );
        }
        StoreAndConfig storeAndConfig = new StoreAndConfig( topLevelStoreDir, neo4jConfigFile );

        System.out.println( "Executing store augmentation step..." );
        Instant augmentStart = Instant.now();
        augmenterizer.augment( threads, storeAndConfig );
        Duration augmentDuration = Duration.between( augmentStart, Instant.now() );
        System.out.println( "Store augmentation step took: " + durationToString( augmentDuration ) );

        config.serialize( topLevelStoreDir.resolve( CONFIG_FILENAME ) );
        return storeAndConfig;
    }

    private StoreAndConfig getCopyOf(
            BenchmarkGroup benchmarkGroup,
            Benchmark benchmark,
            StoreAndConfig storeAndConfig )
    {
        System.out.println( "Reusing copy of store...\n" +
                            "  > Benchmark group: " + benchmarkGroup.name() + "\n" +
                            "  > Benchmark:       " + benchmark.name() + "\n" +
                            "  > Original store:  " + storeAndConfig.store().topLevelDirectory().toAbsolutePath() + "\n" +
                            "  > Config:          " + storeAndConfig.config().toAbsolutePath() );
        Path newTopLevelDir = getCopyOf( storeAndConfig.store().topLevelDirectory() );
        System.out.println( "Copied: " + bytesToString( storeAndConfig.store().bytes() ) + "\n" +
                            "  > Store copy:      " + newTopLevelDir.toAbsolutePath() );
        return new StoreAndConfig( newTopLevelDir, storeAndConfig.config() );
    }

    private Path getCopyOf( Path from )
    {
        Path to = randomTopLevelStoreDir();
        try
        {
            CopyDirVisitor visitor = new CopyDirVisitor( from, to );
            Files.walkFileTree( from, visitor );
            visitor.awaitCompletion();
            Path tempStoreCopyMarkerFile = to.resolve( TEMP_STORE_COPY_MARKER_FILENAME );
            Files.createFile( tempStoreCopyMarkerFile );
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

    public void deleteTemporaryStoreCopies()
    {
        for ( Path temporaryStore : findAllTemporaryStoreCopies( storesDir ) )
        {
            try
            {
                System.out.println( "Deleting: " + temporaryStore.toAbsolutePath() );
                FileUtils.deleteRecursively( temporaryStore.toFile() );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( "Could not delete store: " + temporaryStore, e );
            }
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
        StoreUsage storeUsage = StoreUsage.loadOrCreateIfAbsent( storesDir );
        for ( Path topLevelDir : findAllTopLevelDirs( storesDir ) )
        {
            sb.append( format( "\t%1$-20s %2$s\n", bytesToString( BenchmarkUtil.bytes( topLevelDir ) ), topLevelDir.toAbsolutePath() ) );
            for ( String benchmarkName : storeUsage.benchmarksUsingStore( topLevelDir.getFileName().toString() ) )
            {
                sb.append( "\t\t" ).append( benchmarkName ).append( "\n" );
            }
        }
        return sb
                .append( "-----------------------------------------------------------------------------------------\n" )
                .toString();
    }

    // returns stores that are supposed to be temporary copies -- used to clean up after a benchmark crashes
    private List<Path> findAllTemporaryStoreCopies( Path storesDir )
    {
        return findAllTopLevelDirs( storesDir ).stream()
                                               .filter( topLevelDir -> Files.exists( topLevelDir.resolve( TEMP_STORE_COPY_MARKER_FILENAME ) ) )
                                               .collect( toList() );
    }

    // returns all stores with equal configs, where 'equal' means equal in all ways except
    //  (1) re-usability
    //  (2) that neither config has been augmented
    // if either (new or existing) config is not reusable, persisted config will be overwritten as not reusable
    private List<Path> findAllStoresMatchingConfig( DataGeneratorConfig config, Path storesDir )
    {
        return findAllTopLevelDirs( storesDir ).stream()
                                               .filter( topLevelDir -> storeMatchesConfig( config, topLevelDir ) )
                                               .collect( toList() );
    }

    private static boolean storeMatchesConfig( DataGeneratorConfig config, Path topLevelDir )
    {
        Path storeConfigFile = topLevelDir.resolve( CONFIG_FILENAME );
        return Files.exists( storeConfigFile ) &&
               DataGeneratorConfig.from( storeConfigFile ).equals( config );
    }

    private List<Path> findAllTopLevelDirs( Path storesDir )
    {
        try ( Stream<Path> entries = Files.list( storesDir ) )
        {
            return entries.filter( Stores::isTopLevelDir ).collect( toList() );
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

    public static class StoreAndConfig
    {
        private final Store store;
        private final Path config;

        private StoreAndConfig( Path topLevelStoreDir, Path config )
        {
            this.store = Neo4jStore.createFrom( topLevelStoreDir );
            this.config = config;
        }

        public Store store()
        {
            return store;
        }

        public Path config()
        {
            return config;
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

        static StoreUsage loadOrCreateIfAbsent( Path dir )
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
                JsonUtil.serializeJson( jsonPath );
                return storeUsage;
            }
            else
            {
                return JsonUtil.deserializeJson( jsonPath, StoreUsage.class );
            }
        }

        private Path jsonPath;
        private Map<String,Set<String>> storeBenchmarks;

        /**
         * WARNING: Never call this explicitly.
         * No-params constructor is only used for JSON (de)serialization.
         */
        private StoreUsage()
        {
            this( null );
        }

        private StoreUsage( Path jsonPath )
        {
            this.jsonPath = jsonPath;
            this.storeBenchmarks = new HashMap<>();
        }

        /**
         * 'Registering' a benchmark with a store means that the store was used by that benchmark.
         *
         * @throws IllegalStateException if benchmark had already been registered previously.
         */
        void register( String storeName, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
        {
            Set<String> benchmarks = storeBenchmarks.computeIfAbsent( storeName, name -> new HashSet<>() );
            String benchmarkName = FullBenchmarkName.from( benchmarkGroup, benchmark ).name();
            if ( !benchmarks.add( benchmarkName ) )
            {
                throw new IllegalStateException( format( "Benchmark '%s' was already registered to store '%s'", benchmarkName, storeName ) );
            }
            JsonUtil.serializeJson( jsonPath, this );
        }

        /**
         * @return all store-to-benchmark mappings
         */
        Map<String,Set<String>> allStoreBenchmarkInfo()
        {
            return storeBenchmarks;
        }

        /**
         * @return all benchmarks registered as using the provided store
         */
        Iterable<String> benchmarksUsingStore( String storeName )
        {
            return storeBenchmarks.get( storeName );
        }
    }
}
