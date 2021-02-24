/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.clusterdockertests;

import com.neo4j.server.enterprise.EnterpriseEntryPoint;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * This class provides tooling to load the test-time classpath into the Neo4j Docker Containers being used with the @NeedsCausalCluster annotation. That means
 * that we can rapidly iterate on these tests and neo4j code without needing to regularly build docker containers or leave our IDE.
 * <p>
 * The process of scanning the classpath and loading the files into the Neo4j docker container adds overhead but only has to be done once per test JVM and it
 * takes about 30s on my 2017 13" Mac book pro.
 * <p>
 * By default the {@link DeveloperWorkflow#configureNeo4jContainerIfNecessary} method will use the "developer" mode of loading the runtime classpath into the
 * docker container _unless_ it is explicitly disabled by setting the {@link DeveloperWorkflow#LOAD_RUNTIME_CLASSES_ENV_VAR_NAME} environment variable to
 * "false" when running the tests (e.g. we set this to "false" in TeamCity where the docker image being used already contains the desired code).
 * <p>
 * Suggested usage is:
 * <pre>
 * @NeedsCausalCluster
 * @TestInstance( TestInstance.Lifecycle.PER_CLASS )
 * public class ExampleTest
 * {
 *     @CausalCluster
 *     private static Neo4jCluster cluster;
 *
 *     @CoreModifier
 *     private static Neo4jContainer<?> configure( Neo4jContainer<?> input ) throws IOException
 *     {
 *         return DeveloperWorkflow.configureNeo4jContainerIfNecessary( input );
 *     }
 *
 *     @Test
 *     public void myTest()
 *     {
 *         // when
 *         doSomethingTo(cluster)
 *
 *         // then
 *         assertSomethingAbout(cluster)
 *     }
 * }
 * </pre>
 */
public final class DeveloperWorkflow
{
    private DeveloperWorkflow() throws Exception
    {
        throw new Exception( "This class is not intended to be instantiated" );
    }

    public static Neo4jContainer<?> configureNeo4jContainerIfNecessary( Neo4jContainer<?> input ) throws IOException
    {
        return configureNeo4jContainerIfNecessary( input, false );
    }

    public static Neo4jContainer<?> configureNeo4jContainerIfNecessary( Neo4jContainer<?> input, boolean force ) throws IOException
    {
        // TODO: add some logging here to alert the user to what's happening - requires caller to pass us a logger I think
        // (either loading the current classpath into the container or skipping because of an env var).
        return (force || LOAD_RUNTIME_CLASSES) ? configureNeo4jWithCurrentClasspath( input ) : input;
    }

    /**
     * This is the entrypoint used by neo4j enterprise. Including the class here ensures that everything Neo4j needs to run is on the current classpath.
     */
    private static final EnterpriseEntryPoint clazzTrickery = null;

    /**
     * This is a list of know "testing" related jar file prefixes. It's not necessary to exclude them but it's a performance optimisation
     */
    private static final Set<String> EXCLUDED_JAR_PREFIXES = Set.of( "idea", "junit", "junit4", "junit5", "assertj", "testcontainers", "hamcrest" );

    /**
     * Extra bash to run in the docker container just before the neo4j process is started. If the container is stopped/killed and then started again these will
     * re-run but file system changes are preserved - so the commands must be safe to re-run multiple times.
     */
    private static final String EXTENSION_SCRIPT_BASH =
            // if there is a neo4j-browser jar in the docker image already copy it to the devlib directory.
            // browser is an optional runtime dependency that isn't part of the monorepo but it's helpful for debugging to keep it around.
            "find '/var/lib/neo4j/lib/' -type f -iname 'neo4j-browser*.jar' -exec mv -t /var/lib/neo4j/devlib/ {} + >/dev/null\n" +

            // delete all the jars that came with the docker image (if there are still any present)
            "( ! compgen -G '/var/lib/neo4j/lib/*' >/dev/null ) || rm /var/lib/neo4j/lib/*\n" +

            // link the mounted jars to the place that the neo4j startup script expects them to be
            "ln -s /var/lib/neo4j/devlib/* /var/lib/neo4j/lib/\n" +

            // print something to stdout - we use this check the logs to be sure that the dev workflow was run.
            "echo 'dev extension script completed.'\n";

    private static final String EXTENTION_SCRIPT_LOCATION = "/developerworkflow.sh";

    /**
     * Environment variable name. Set this to "false" to disable the loading of runtime classes into the docker container (the default behaviour). In general
     * the only reason to set this to "false" is if you specifically want to test a docker image rather than the code in this repo.
     */
    private static final String LOAD_RUNTIME_CLASSES_ENV_VAR_NAME = "CLUSTER_DOCKER_TESTS_LOAD_RUNTIME_CLASSES";
    private static final Boolean LOAD_RUNTIME_CLASSES = Boolean.parseBoolean(
            System.getenv().getOrDefault( LOAD_RUNTIME_CLASSES_ENV_VAR_NAME, "true" ) );

    // static variables to hold lazily-instantiated values
    private static volatile MountableFile lazyScriptToMount;
    private static volatile List<MountableFile> lazyJarsToMount;

    private static synchronized MountableFile getScriptToMount() throws IOException
    {
        lazyScriptToMount = lazyScriptToMount == null ? MountableFile.forHostPath( createExtensionScript() ) : lazyScriptToMount;
        return lazyScriptToMount;
    }

    private static synchronized List<MountableFile> getJarsToMount()
    {
        lazyJarsToMount = lazyJarsToMount == null ? createJarsToMountFromCurrentClasspath() : lazyJarsToMount;
        return lazyJarsToMount;
    }

    private static Path createExtensionScript() throws IOException
    {
        Path f = Files.createTempFile( "extension", ".sh" );
        Files.write( f, EXTENSION_SCRIPT_BASH.getBytes() );
        return f;
    }

    private static Neo4jContainer<?> configureNeo4jWithCurrentClasspath( Neo4jContainer<?> input ) throws IOException
    {
        for ( MountableFile toLoad : getJarsToMount() )
        {
            String fileName = Path.of( toLoad.getFilesystemPath() ).getFileName().toString();
            input = input.withCopyFileToContainer( toLoad, "/var/lib/neo4j/devlib/" + fileName );
        }

        return input.withCopyFileToContainer( getScriptToMount(), EXTENTION_SCRIPT_LOCATION )
                    .withEnv( "EXTENSION_SCRIPT", EXTENTION_SCRIPT_LOCATION )
                    .withEnv( "NEO4J_dbms_directories_lib", "devlib" )
                    .withStartupTimeout( Duration.ofMinutes( 5 ) );
    }

    /**
     * This method searches the current classpath for .class and .jar files. All class files that are found are added to an "uber jar". The resulting jar files
     * (including the uber jar) are wrapped in MountableFiles and returned.
     * <p>
     * This allows us to "Mount" everything on the current classpath into the Neo4jContainer.
     *
     * @return a list of MountableFiles that can be used in the Neo4jContainer.
     */
    private static List<MountableFile> createJarsToMountFromCurrentClasspath()
    {
        List<MountableFile> jarsToLoad = new LinkedList<>();
        try
        {
            Path f = Files.createTempFile( "uber", ".jar" );

            try ( UberJar uberJar = new UberJar( f ) )
            {
                uberJar.start();
                findClasses( c ->
                             {
                                 String filename = c.getFileName().toString();
                                 if ( filename.endsWith( ".jar" ) )
                                 {
                                     jarsToLoad.add( MountableFile.forHostPath( c ) );
                                 }
                                 else if ( filename.endsWith( ".class" ) )
                                 {
                                     uberJar.addClass( c );
                                 }
                             } );

                uberJar.writeServiceDeclarations();
            }
            jarsToLoad.add( MountableFile.forHostPath( f ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        return jarsToLoad;
    }

    private static void findClasses( Consumer<Path> visitor )
    {
        String classpath = System.getProperty( "java.class.path" );
        String[] paths = classpath.split( System.getProperty( "path.separator" ) );

        for ( String path : paths )
        {
            findClasses( Path.of( path ), visitor );
        }
    }

    private static void findClasses( final Path file, Consumer<Path> visitor )
    {
        if ( file == null || !Files.exists( file ) )
        {
            return;
        }

        if ( Files.isDirectory( file ) && !file.toString().endsWith( "test-classes" ) )
        {
            try
            {
                Files.list( file ).forEach( child -> findClasses( child, visitor ) );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
        else
        {
            String filename = file.getFileName().toString().toLowerCase();

            // TODO: This could be improved by inspecting jars and filtering on package names.
            if ( filename.endsWith( ".jar" ) && !EXCLUDED_JAR_PREFIXES.contains( filename.split( "[-_]" )[0] ) )
            {
                visitor.accept( file );
            }
            else if ( filename.endsWith( ".class" ) )
            {
                visitor.accept( file );
            }
        }
    }
}
