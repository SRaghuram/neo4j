/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.profiling.FullBenchmarkName;
import com.neo4j.bench.client.util.BenchmarkUtil;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.Store;
import com.neo4j.bench.micro.benchmarks.Kaboom;
import com.neo4j.commercial.edition.factory.CommercialDatabaseManagementServiceBuilder;

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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.SettingImpl;
import org.neo4j.io.fs.FileUtils;

import static com.neo4j.bench.client.util.BenchmarkUtil.bytesToString;
import static com.neo4j.bench.client.util.BenchmarkUtil.durationToString;
import static com.neo4j.bench.client.util.BenchmarkUtil.forceRecreateFile;
import static com.neo4j.bench.client.util.BenchmarkUtil.tryMkDir;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.STRING;

public class Stores
{
    private static final String NEO4J_CONFIG_FILENAME_SUFFIX = "__neo4j.conf";
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

    public Path storesDir()
    {
        return storesDir;
    }

    public Neo4jConfig neo4jConfigFor( BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        return Neo4jConfigBuilder.fromFile( findNeo4jConfigFor( FullBenchmarkName.from( benchmarkGroup, benchmark ) ) ).build();
    }

    StoreAndConfig prepareDb(
            DataGeneratorConfig config,
            BenchmarkGroup group,
            Benchmark benchmark,
            Augmenterizer augmenterizer,
            int threads )
    {
        List<Path> topLevelDirs = findAllStoresMatchingConfig( config, storesDir );
        FullBenchmarkName benchmarkName = FullBenchmarkName.from( group, benchmark );

        if ( topLevelDirs.isEmpty() )
        {
            StoreAndConfig initialStoreAndConfig = generateDb(
                    config,
                    augmenterizer,
                    benchmarkName,
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
            Path neo4jConfig = getOrCreateNeo4jConfigFor( topLevelDir, benchmarkName, config.neo4jConfig() );

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
                                    "  > Config:          " + neo4jConfig.toAbsolutePath() );
                return new StoreAndConfig( topLevelDir, neo4jConfig );
            }
            else
            {
                return getCopyOf( group, benchmark, new StoreAndConfig( topLevelDir, neo4jConfig ) );
            }
        }
        else
        {
            throw new IllegalStateException( format( "Found too many stores for config\n" +
                                                     "Stores: %s\n" +
                                                     "%s", topLevelDirs, config ) );
        }
    }

    public void writeNeo4jConfigForNoStore( Neo4jConfig neo4jConfig, FullBenchmarkName benchmarkName )
    {
        Path topLevelDir = storesDir.resolve( NULL_STORE_DIR_NAME );
        tryMkDir( topLevelDir );
        writeNeo4jConfig( neo4jConfig, benchmarkName, topLevelDir );
    }

    private Path writeNeo4jConfig( Neo4jConfig neo4jConfig, FullBenchmarkName benchmarkName, Path topLevelDir )
    {
        Path neo4jConfigFile = topLevelDir.resolve( benchmarkName.sanitizedName() + NEO4J_CONFIG_FILENAME_SUFFIX );
        System.out.println( "\nWriting Neo4j config to: " + neo4jConfigFile.toAbsolutePath() );
        forceRecreateFile( neo4jConfigFile );
        Neo4jConfigBuilder.writeToFile( neo4jConfig, neo4jConfigFile );
        return neo4jConfigFile;
    }

    private StoreAndConfig generateDb(
            DataGeneratorConfig config,
            Augmenterizer augmenterizer,
            FullBenchmarkName benchmarkName,
            int threads )
    {
        Path topLevelStoreDir = randomTopLevelStoreDir();
        tryMkDir( topLevelStoreDir );

        // store Neo4j config every time, even if DataGeneratorConfig is identical -- they are retrieved later
        Path neo4jConfig = writeNeo4jConfig( config.neo4jConfig(), benchmarkName, topLevelStoreDir );

        System.out.println( "Generating store in: " + topLevelStoreDir.toAbsolutePath() );
        System.out.println( config );

        // will create an empty database directory under top level
        new CommercialDatabaseManagementServiceBuilder( topLevelStoreDir.toFile() )
                .setConfig( config.neo4jConfig().toMap().entrySet().stream()
                        .collect( Collectors.toMap( e -> SettingImpl.newBuilder( e.getKey(), STRING, null ).build(), Map.Entry::getValue ) ) )
                .build()
                .shutdown();

        try
        {
            new DataGenerator( config ).generate( Store.createFrom( topLevelStoreDir ), neo4jConfig );
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
        StoreAndConfig storeAndConfig = new StoreAndConfig( topLevelStoreDir, neo4jConfig );

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
        for ( Path topLevelDir : findAllTopLevelDirs( storesDir ) )
        {
            sb.append( format( "\t%1$-20s %2$s\n", bytesToString( BenchmarkUtil.bytes( topLevelDir ) ), topLevelDir.toAbsolutePath() ) );
            for ( String benchmarkName : namesOfBenchmarksThatUseStore( topLevelDir ) )
            {
                sb.append( "\t\t" ).append( benchmarkName ).append( "\n" );
            }
        }
        return sb
                .append( "-----------------------------------------------------------------------------------------\n" )
                .toString();
    }

    // TODO why is this never called anymore?
    private Path findNeo4jConfigFor( FullBenchmarkName benchmarkName )
    {
        List<Path> neo4jConfigs = findAllTopLevelDirs( storesDir ).stream()
                                                                  .map( store -> store.resolve( benchmarkName.sanitizedName() + NEO4J_CONFIG_FILENAME_SUFFIX ) )
                                                                  .filter( Files::exists )
                                                                  .collect( toList() );
        if ( neo4jConfigs.isEmpty() )
        {
            throw new Kaboom( "Could not find Neo4j config file for: " + benchmarkName.sanitizedName() );
        }
        else if ( neo4jConfigs.size() == 1 )
        {
            return neo4jConfigs.get( 0 );
        }
        else
        {
            throw new Kaboom( "Found multiple Neo4j config files for: " + benchmarkName.sanitizedName() + "\n" +
                              neo4jConfigs.stream().map( Path::toString ).collect( joining( "\n" ) ) );
        }
    }

    private Path getOrCreateNeo4jConfigFor( Path topLevelDir, FullBenchmarkName benchmarkName, Neo4jConfig neo4jConfig )
    {
        Path neo4jConfigFile = topLevelDir.resolve( benchmarkName.sanitizedName() + NEO4J_CONFIG_FILENAME_SUFFIX );
        if ( !Files.exists( neo4jConfigFile ) )
        {
            writeNeo4jConfig( neo4jConfig, benchmarkName, topLevelDir );
        }
        return neo4jConfigFile;
    }

    private List<String> namesOfBenchmarksThatUseStore( Path topLevelDir )
    {
        try ( Stream<Path> entries = Files.list( topLevelDir ) )
        {
            return entries
                    .filter( p -> p.toString().endsWith( NEO4J_CONFIG_FILENAME_SUFFIX ) )
                    .map( p -> p.getFileName().toString() )
                    .map( name -> name.substring( 0, name.length() - NEO4J_CONFIG_FILENAME_SUFFIX.length() ) )
                    .collect( toList() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
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
        Path dbDir = topStoreLevelDir.resolve( DEFAULT_DATABASE_NAME );
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

    public class StoreAndConfig
    {
        private final Store store;
        private final Path config;

        private StoreAndConfig( Path topLevelStoreDir, Path config )
        {
            this.store = Store.createFrom( topLevelStoreDir );
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
            executorService = Executors.newFixedThreadPool( 6 );
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
}
