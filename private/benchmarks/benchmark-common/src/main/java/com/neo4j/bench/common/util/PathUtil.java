/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.util;

import com.google.common.base.Strings;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static java.lang.String.format;

public class PathUtil
{
    /**
     * Maximum filename length on most of popular filesystems (e.g. ext4, zfs, NTFS, APFS).
     *
     * @see <a href="https://en.wikipedia.org/wiki/Comparison_of_file_systems#Limits">Comparison on Wikipedia</a>.
     */
    private static final int DEFAULT_MAX_LENGTH = 255;
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    private static final char SEPARATOR = '_';

    private final int maxLength;

    public static PathUtil withDefaultMaxLength()
    {
        return new PathUtil( DEFAULT_MAX_LENGTH );
    }

    public static PathUtil withDefaultMaxLengthAndMargin( int margin )
    {
        return new PathUtil( DEFAULT_MAX_LENGTH - margin );
    }

    static PathUtil withMaxLength( int maxLength )
    {
        return new PathUtil( maxLength );
    }

    private PathUtil( int maxLength )
    {
        this.maxLength = maxLength;
    }

    public String limitLength( String filename )
    {
        return limitLength( "", filename, "" );
    }

    public String limitLength( String filename, String extension )
    {
        return limitLength( "", filename, extension );
    }

    public String limitLength( String prefix, String filename, String extension )
    {
        String hash = md5Sum( filename );
        String suffix = SEPARATOR + hash + extension;

        String fullFilename = prefix + filename + suffix;
        if ( fullFilename.length() <= maxLength )
        {
            return fullFilename;
        }
        int surplusLength = fullFilename.length() - maxLength;
        if ( surplusLength >= filename.length() )
        {
            throw new IllegalArgumentException( format( "Cannot truncate %s (length: %d) to %d characters", fullFilename, fullFilename.length(), maxLength ) );
        }
        return prefix + filename.substring( 0, filename.length() - surplusLength ) + suffix;
    }

    private String md5Sum( String s )
    {
        MessageDigest digest = createMd5MessageDigest();
        digest.update( s.getBytes( CHARSET ) );
        return toHexadecimalString( digest );
    }

    private MessageDigest createMd5MessageDigest()
    {
        try
        {
            return MessageDigest.getInstance( "MD5" );
        }
        catch ( NoSuchAlgorithmException e )
        {
            throw new RuntimeException( "Could not create instance of MD5 digest", e );
        }
    }

    private String toHexadecimalString( MessageDigest digest )
    {
        BigInteger integer = new BigInteger( 1, digest.digest() );
        String asHexadecimal = integer.toString( 16 );
        return Strings.padStart( asHexadecimal, 32, '0' );
    }
}
