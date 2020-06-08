/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.util;

import org.apache.commons.lang3.SystemUtils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static java.lang.String.format;

public class Jvm
{
    private static final String JAVA_HOME = "JAVA_HOME";
    private static final String JAVA_EXECUTABLE = osSpecificExecutable( "java" );
    private static final String JAVAC_EXECUTABLE = osSpecificExecutable( "javac" );

    /**
     * Try to find JVM at provided path.
     * If path does not point at valid java, try to find java via JAVA_HOME.
     * If JAVA_HOME is not set, fail.
     *
     * @param jvmPath
     * @return JVM domain object
     */
    public static Jvm bestEffortOrFail( File jvmPath )
    {
        return bestEffortOrFail( null == jvmPath ? null : jvmPath.toPath() );
    }

    /**
     * Try to find JVM at provided path.
     * If path does not point at valid java, try to find java via JAVA_HOME.
     * If JAVA_HOME is not set, fail.
     *
     * @param jvmPath
     * @return JVM domain object
     */
    public static Jvm bestEffortOrFail( Path jvmPath )
    {
        if ( null != jvmPath )
        {
            assertValidJvmPath( jvmPath );
            return new Jvm( jvmPath.toAbsolutePath().toString() );
        }
        else
        {
            return defaultJvmOrFail();
        }
    }

    /**
     * Try to find JVM at provided path.
     * If path does not point at valid java, try to find java via JAVA_HOME.
     * If JAVA_HOME is not set, simply use 'java' (rather than path) when launching a java process.
     *
     * @param jvmPath
     * @return JVM domain object
     */
    public static Jvm bestEffort( File jvmPath )
    {
        return bestEffort( null == jvmPath ? null : jvmPath.toPath() );
    }

    /**
     * Try to find JVM at provided path.
     * If path does not point at valid java, try to find java via JAVA_HOME.
     * If JAVA_HOME is not set, simply use 'java' (rather than path) when launching a java process.
     *
     * @param jvmPath
     * @return JVM domain object
     */
    public static Jvm bestEffort( Path jvmPath )
    {
        if ( null != jvmPath )
        {
            assertValidJvmPath( jvmPath );
            return new Jvm( jvmPath.toAbsolutePath().toString() );
        }
        else
        {
            return defaultJvm();
        }
    }

    /**
     * Try to find JVM via JAVA_HOME.
     * If JAVA_HOME is not set, fail.
     *
     * @return JVM domain object
     */
    public static Jvm defaultJvmOrFail()
    {
        String jvmPath = maybeDefaultPath()
                .map( Path::toAbsolutePath )
                .map( Path::toString )
                .orElseThrow( () -> new RuntimeException( "No default found, 'JAVA_HOME' not set" ) );
        return new Jvm( jvmPath );
    }

    /**
     * Try to find JVM via JAVA_HOME.
     * If JAVA_HOME is not set, simply use 'java' (rather than path) when launching a java process.
     *
     * @return JVM domain object
     */
    public static Jvm defaultJvm()
    {
        String jvmPath = maybeDefaultPath()
                .map( Path::toAbsolutePath )
                .map( Path::toString )
                .orElse( JAVA_EXECUTABLE );
        return new Jvm( jvmPath );
    }

    /**
     * It tries to find JVM in a JDK directory
     * @param jdkPath
     * @return
     */
    public static Jvm fromJdkPath( Path jdkPath )
    {
        return bestEffort( jdkPath.resolve( "bin" ).resolve( JAVA_EXECUTABLE ) );
    }

    private static Optional<Path> maybeDefaultPath()
    {
        String javaHome = System.getenv( JAVA_HOME );
        if ( null == javaHome )
        {
            return Optional.empty();
        }
        else
        {
            Path javaHomeDir = Paths.get( javaHome );
            BenchmarkUtil.assertDirectoryExists( javaHomeDir );
            Path javaBin = javaHomeDir.resolve( format( "bin/%s", JAVA_EXECUTABLE ) );
            BenchmarkUtil.assertFileExists( javaBin );
            return Optional.of( javaBin );
        }
    }

    /**
     * @param fileName file fileName to return OS-specific file fileName for, e.g. java -> java.exe on Windows.
     * @return an OS-specific version of the given {@code fileName}.
     */
    private static String osSpecificExecutable( String fileName )
    {
        return SystemUtils.IS_OS_WINDOWS ? fileName + ".exe" : fileName;
    }

    /*
    Miscellaneous sanity checks to make sure the provided path actually points to java
     */
    private static void assertValidJvmPath( Path jvmPath )
    {
        if ( null == jvmPath )
        {
            throw new RuntimeException( "Path to JVM is null" );
        }
        BenchmarkUtil.assertFileExists( jvmPath );
        if ( !JAVA_EXECUTABLE.equalsIgnoreCase( jvmPath.getFileName().toString() ) )
        {
            throw new RuntimeException( format( "Expected JVM path to point to `%s` but was: %s", JAVA_EXECUTABLE, jvmPath.toAbsolutePath() ) );
        }
        Path binDir = jvmPath.getParent();
        if ( !"bin".equalsIgnoreCase( binDir.getFileName().toString() ) )
        {
            throw new RuntimeException( format( "Expected java to be in 'bin/' folder but was in: %s", binDir.toAbsolutePath() ) );
        }
        if ( !Files.exists( binDir.resolve( JAVAC_EXECUTABLE ) ) )
        {
            throw new RuntimeException( format( "Expected '%s' to be in same folder as '%s'", JAVAC_EXECUTABLE, JAVA_EXECUTABLE ) );
        }
    }

    private final String jvmPath;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    private Jvm()
    {
        this( null );
    }

    private Jvm( String jvmPath )
    {
        this.jvmPath = jvmPath;
    }

    /**
     * Specifies whether or not this instance contains an absolute path pointing to a java installation.
     *
     * @return true, if instance points to a java installation.
     */
    public boolean hasPath()
    {
        return !JAVA_EXECUTABLE.equals( jvmPath );
    }

    /**
     * @return absolute path pointing to a java installation, or simply 'java' if this instance does not contain a path.
     */
    public String launchJava()
    {
        return jvmPath;
    }

    public Optional<Path> jdkPath()
    {
        return asPath().map( p -> p.getParent().getParent() );
    }

    public String launchJcmd()
    {
        return jdkTool( "jcmd" );
    }

    public String launchJps()
    {
        return jdkTool( "jps" );
    }

    public JvmVersion version()
    {
        return JvmVersion.getVersion( this );
    }

    @Override
    public String toString()
    {
        return launchJava();
    }

    /**
     * @return absolute path pointing to a java installation.
     * @throws RuntimeException if this instance does not point to a java installation.
     */
    private Optional<Path> asPath()
    {
        if ( !hasPath() )
        {
            return Optional.empty();
        }
        else
        {
            return Optional.of( Paths.get( jvmPath ) );
        }
    }

    private String jdkTool( String tool )
    {
        return jdkPath()
                .map( p -> p.resolve( "bin" ).resolve( tool ).toAbsolutePath().toString() )
                .orElse( tool );
    }
}
