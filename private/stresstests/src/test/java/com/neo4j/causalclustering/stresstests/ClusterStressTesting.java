/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.helper.Workload;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.function.ThrowingAction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.Log;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

@ExtendWith( {DefaultFileSystemExtension.class, PageCacheSupportExtension.class} )
class ClusterStressTesting
{
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final PageCacheRule pageCacheRule = new PageCacheRule();

    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;

    @BeforeEach
    void setup()
    {
        fileSystem = fileSystemRule.get();
        pageCache = pageCacheRule.getPageCache( fileSystem );
    }

    @Test
    void shouldBehaveCorrectlyUnderStress() throws Exception
    {
        stressTest( new Config(), fileSystem, pageCache );
    }

    static void stressTest( Config config, FileSystemAbstraction fileSystem, PageCache pageCache ) throws Exception
    {
        Resources resources = new Resources( fileSystem, pageCache, config );
        Control control = new Control( config );
        Log log = config.logProvider().getLog( ClusterStressTesting.class );

        log.info( config.toString() );

        List<Preparation> preparations = config.preparations().stream()
                .map( preparation -> preparation.create( resources ) )
                .collect( Collectors.toList() );

        List<Workload> workloads = config.workloads().stream()
                .map( workload -> workload.create( control, resources, config ) )
                .collect( Collectors.toList() );

        List<Validation> validations = config.validations().stream()
                .map( validator -> validator.create( resources ) )
                .collect( Collectors.toList() );

        if ( workloads.size() == 0 )
        {
            throw new IllegalArgumentException( "No workloads." );
        }

        ExecutorService executor = Executors.newCachedThreadPool();

        try
        {
            log.info( "Starting resources" );
            resources.start();

            log.info( "Preparing scenario" );
            for ( Preparation preparation : preparations )
            {
                preparation.prepare();
            }

            log.info( "Preparing workloads" );
            for ( Workload workload : workloads )
            {
                workload.prepare();
            }

            log.info( "Starting workloads" );
            List<Future<?>> completions = new ArrayList<>();
            for ( Workload workload : workloads )
            {
                completions.add( executor.submit( workload ) );
            }

            control.awaitEnd( completions );

            for ( Workload workload : workloads )
            {
                workload.validate();
            }
        }
        catch ( Throwable e )
        {
            control.onFailure( e );
        }

        catchFailures( control, () ->
        {
            log.info( "Shutting down executor" );
            executor.shutdownNow();
            executor.awaitTermination( 5, TimeUnit.MINUTES );
            log.info( "Shut down of executor complete" ); // Control suppresses interrupted exceptions, so we have an additional print here for traceability.
        } );

        log.info( "Validating results pre-stop" );
        validations
                .stream()
                .filter( v -> !v.postStop() )
                .forEach( validation -> catchFailures( control, validation::validate ) );

        log.info( "Stopping resources" );
        catchFailures( control, resources::stop );

        log.info( "Validating results post-stop" );
        validations
                .stream()
                .filter( Validation::postStop )
                .forEach( validation -> catchFailures( control, validation::validate ) );

        control.assertNoFailure(); // Will throw and skip clean up if there were any failures, leaving the files for post-mortem.

        log.info( "Cleaning up" );
        resources.cleanup();
    }

    private static void catchFailures( Control control, ThrowingAction<Exception> task )
    {
        try
        {
            task.apply();
        }
        catch ( Throwable e )
        {
            control.onFailure( e );
        }
    }
}
