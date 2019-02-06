/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.util;

import com.google.common.base.CharMatcher;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;

public class JvmVersion
{
    private static final String RELEASE_FILE = "release";
    private static final String JAVA_VM_VENDOR_RUNTIME_PROPERTY = "java.vm.vendor = ";
    private static final String JAVA_VERSION_RUNTIME_PROPERTY = "java.version = ";
    private static final String JAVA_VERSION_RELEASE_PROPERTY = "JAVA_VERSION";
    private static final String IMPLEMENTOR_RELEASE_PROPERTY = "IMPLEMENTOR";

    public static JvmVersion create( int majorVersion, String implementor )
    {
        return new JvmVersion( majorVersion, implementor );
    }

    static JvmVersion getVersion( Jvm jvm )
    {
        return jvm.jdkPath()
                .flatMap( JvmVersion::getVersionFromRelease )
                .orElseGet( () -> getVersionFromRuntime( jvm ) );
    }

    static Optional<JvmVersion> getVersionFromRelease( Path jdkPath )
    {
        Path releaseFile = jdkPath.resolve( RELEASE_FILE );
        if ( Files.exists( releaseFile ) )
        {
            Properties release = toProperties( releaseFile );

            String javaVersionRaw = release.getProperty( JAVA_VERSION_RELEASE_PROPERTY );
            if ( StringUtils.isEmpty( javaVersionRaw ) )
            {
                throw new RuntimeException( String.format( "Invalid %s file, should contain \"%s\" property", releaseFile, JAVA_VERSION_RELEASE_PROPERTY ) );
            }
            int majorVersion = parseMajorVersion( CharMatcher.is( '\"' ).trimFrom( javaVersionRaw ) );

            String implementorRaw = release.getProperty( IMPLEMENTOR_RELEASE_PROPERTY );
            String implementor = null;
            if ( StringUtils.isNotEmpty( implementorRaw ) )
            {
                implementor = CharMatcher.is( '\"' ).trimFrom( implementorRaw );
            }
            return Optional.of( new JvmVersion( majorVersion, implementor ) );
        }
        else
        {
            throw new RuntimeException( String.format( "Invalid JDK, release file %s not found", releaseFile ) );
        }
    }

    static JvmVersion getVersionFromRuntime( Jvm jvm )
    {
        String jvmPath = jvm.launchJava();
        ProcessBuilder processBuilder = new ProcessBuilder( jvm.launchJava(), "-XshowSettings:properties", "-version" )
                .redirectOutput( Redirect.PIPE )
                .redirectErrorStream( true );
        try
        {
            Process process = processBuilder.start();
            int exitCode = process.waitFor();
            if ( exitCode == 0 )
            {
                List<String> lines = IOUtils.readLines( process.getInputStream(), Charset.defaultCharset() );

                // majorVersion is obligatory
                int majorVersion = lines.stream()
                        .filter( line -> line.contains( JAVA_VERSION_RUNTIME_PROPERTY ) )
                        .findAny()
                        .flatMap( JvmVersion::findJdkVersion )
                        .map( JvmVersion::parseMajorVersion )
                        .orElseThrow( () -> new RuntimeException( "Java major version unknown" ) );

                //implementor can be an empty string
                String implementor = lines.stream()
                        .filter( line -> line.contains( JAVA_VM_VENDOR_RUNTIME_PROPERTY ) )
                        .findAny()
                        .flatMap( JvmVersion::findImplementor )
                        .orElse( null );

                return new JvmVersion( majorVersion, implementor );
            }
            throw new RuntimeException( format( "%s process finished with non-zero exit code", jvmPath ) );
        }
        catch ( IOException | InterruptedException e )
        {
            throw new RuntimeException( format( "Unable to start java from %s directory", jvmPath ) );
        }
    }

    static int parseMajorVersion( String version )
    {
        if ( version == null )
        {
            throw new NullPointerException( "version string cannot be null" );
        }
        Matcher m = VersionPattern.VSTR_PATTERN.matcher( version );
        if ( !m.matches() )
        {
            return Integer.parseInt( tryParsePreJdk9Version( version ) );
        }
        String vnumGroup = m.group( VersionPattern.VNUM_GROUP );
        return Integer.parseInt( tryParsePostJdk9Version( vnumGroup ) );
    }

    private static Optional<String> findJdkVersion( String line )
    {
        return getPropertyValue( line, JAVA_VERSION_RUNTIME_PROPERTY );
    }

    private static Optional<String> findImplementor( String line )
    {
        return getPropertyValue( line, JAVA_VM_VENDOR_RUNTIME_PROPERTY );
    }

    private static Optional<String> getPropertyValue( String line, String javaVersionProperty )
    {
        int idx = line.indexOf( javaVersionProperty );
        if ( idx < 0 )
        {
            return Optional.empty();
        }
        return Optional.of( line.substring( idx + javaVersionProperty.length() ) );
    }

    private static String tryParsePostJdk9Version( String s )
    {
        String[] versionStrings = s.split( "\\." );
        return versionStrings[0];
    }

    private static String tryParsePreJdk9Version( String s )
    {
        String[] versionStrings = s.split( "\\." );
        return versionStrings[1];
    }

    private static Properties toProperties( Path releaseFile )
    {
        Properties release = new Properties();
        try
        {
            release.load( Files.newInputStream( releaseFile ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        return release;
    }

    /**
     * As described in https://docs.oracle.com/javase/10/docs/api/java/lang/Runtime.Version.html,
     * unfortunately this class is not available in JDK 8, so we need to have our own
     * implementation.
     *
     */
    private static class VersionPattern
    {
        private static final String VNUM = "(?<VNUM>[1-9][0-9]*(?:(?:\\.0)*\\.[1-9][0-9]*)*)";
        private static final String PRE = "(?:-(?<PRE>[a-zA-Z0-9]+))?";
        private static final String BUILD = "(?:(?<PLUS>\\+)(?<BUILD>0|[1-9][0-9]*)?)?";
        private static final String OPT = "(?:-(?<OPT>[-a-zA-Z0-9.]+))?";
        private static final String VSTR_FORMAT = VNUM + PRE + BUILD + OPT;
        private static final Pattern VSTR_PATTERN = Pattern.compile( VSTR_FORMAT );
        private static final String VNUM_GROUP = "VNUM";
    }

    private final int jvmMajorVersion;
    private final String implementor;

    private JvmVersion( int jvmMajorVersion, String implementor )
    {
        this.jvmMajorVersion = jvmMajorVersion;
        this.implementor = implementor;
    }

    public int majorVersion()
    {
        return jvmMajorVersion;
    }

    public Optional<String> implementor()
    {
        return Optional.ofNullable( implementor );
    }

}
