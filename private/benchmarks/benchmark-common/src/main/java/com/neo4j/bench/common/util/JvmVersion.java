/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.util;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;

public class JvmVersion
{
    public static final String JAVA_TM_SE_RUNTIME_ENVIRONMENT = "Java(TM) SE Runtime Environment";

    private static final String JAVA_RUNTIME_NAME_PROPERTY = "java.runtime.name = ";
    private static final String JAVA_VERSION_PROPERTY = "java.version = ";

    public static JvmVersion create( int majorVersion, String runtimeName )
    {
        return new JvmVersion( majorVersion, runtimeName );
    }

    static JvmVersion getVersion( Jvm jvm )
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
                        .filter( line -> line.contains( JAVA_VERSION_PROPERTY ) )
                        .findAny()
                        .flatMap( JvmVersion::findJdkVersion )
                        .map( JvmVersion::parseMajorVersion )
                        .orElseThrow( () -> new RuntimeException( "Java major version unknown" ) );

                // runtimeName is obligatory
                String runtimeName = lines.stream()
                        .filter( line -> line.contains( JAVA_RUNTIME_NAME_PROPERTY ) )
                        .findAny()
                        .flatMap( JvmVersion::findRuntimeName )
                        .orElseThrow( () -> new RuntimeException( "Java runtime name unknown" ) );

                return new JvmVersion( majorVersion, runtimeName );
            }
            throw new RuntimeException( format( "%s process finished with non-zero exit code", jvmPath ) );
        }
        catch ( IOException | InterruptedException e )
        {
            throw new RuntimeException( format( "Unable to start java from %s directory", jvmPath ) );
        }
    }

    public static int parseMajorVersion( String version )
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
        return getPropertyValue( line, JAVA_VERSION_PROPERTY );
    }

    private static Optional<String> findRuntimeName( String line )
    {
        return getPropertyValue( line, JAVA_RUNTIME_NAME_PROPERTY );
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
    private final String runtimeName;

    private JvmVersion( int jvmMajorVersion, String runtimeName )
    {
        this.jvmMajorVersion = jvmMajorVersion;
        this.runtimeName = runtimeName;
    }

    public int majorVersion()
    {
        return jvmMajorVersion;
    }

    public String runtimeName()
    {
        return runtimeName;
    }

    public boolean hasCommercialFeatures()
    {
        return majorVersion() < 11 &&
                runtimeName().equals( JAVA_TM_SE_RUNTIME_ENVIRONMENT );
    }

}
