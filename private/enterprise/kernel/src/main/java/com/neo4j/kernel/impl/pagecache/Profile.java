/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.pagecache;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PagedFile;

import static com.neo4j.kernel.impl.pagecache.PageCacheWarmer.SUFFIX_CACHEPROF;

final class Profile implements Comparable<Profile>
{
    private final File profileFile;
    private final File pagedFile;
    private final long profileSequenceId;

    private Profile( File profileFile, File pagedFile, long profileSequenceId )
    {
        Objects.requireNonNull( profileFile );
        Objects.requireNonNull( pagedFile );
        this.profileFile = profileFile;
        this.pagedFile = pagedFile;
        this.profileSequenceId = profileSequenceId;
    }

    @Override
    public int compareTo( Profile that )
    {
        int compare = pagedFile.compareTo( that.pagedFile );
        return compare == 0 ? Long.compare( profileSequenceId, that.profileSequenceId ) : compare;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o instanceof Profile )
        {
            Profile profile = (Profile) o;
            return profileFile.equals( profile.profileFile );
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return profileFile.hashCode();
    }

    @Override
    public String toString()
    {
        return "Profile(" + profileSequenceId + " for " + pagedFile + ")";
    }

    File file()
    {
        return profileFile;
    }

    void delete( FileSystemAbstraction fs )
    {
        fs.deleteFile( profileFile );
    }

    InputStream read( FileSystemAbstraction fs ) throws IOException
    {
        InputStream source = fs.openAsInputStream( profileFile );
        try
        {
            return new GZIPInputStream( source );
        }
        catch ( IOException e )
        {
            IOUtils.closeAllSilently( source );
            throw new IOException( "Exception when building decompressor.", e );
        }
    }

    OutputStream write( FileSystemAbstraction fs ) throws IOException
    {
        fs.mkdirs( profileFile.getParentFile() ); // Create PROFILE_FOLDER if it does not exist.
        OutputStream sink = fs.openAsOutputStream( profileFile, false );
        try
        {
            return new BufferedOutputStream( new GZIPOutputStream( sink ) );
        }
        catch ( IOException e )
        {
            IOUtils.closeAllSilently( sink );
            throw new IOException( "Exception when building compressor.", e );
        }
    }

    Profile next()
    {
        long next = profileSequenceId + 1L;
        return new Profile( profileName( profileFile.getParentFile(), pagedFile, next ), pagedFile, next );
    }

    static Profile first( File databaseDirectory, File profileDirectory, File file )
    {
        return new Profile( firstProfileName( databaseDirectory, profileDirectory, file ), file, 0 );
    }

    /**
     * Create profile file for mappedFile. It is assumed that baseDirectory contains mappedFile (can be in multiple sub directories down).
     * Profile file will be placed inside profileDirectory but keep it's sub directory structure relative to baseDirectory.
     */
    private static File firstProfileName( File baseDirectory, File profileDirectory, File mappedFile )
    {
        File profileFileDir = FileUtils.pathToFileAfterMove( baseDirectory, profileDirectory, mappedFile ).getParentFile();
        File file = profileName( profileFileDir, mappedFile, 0L );
        return file;
    }

    /**
     * Create profile file for mappedFile directly inside profileFileDirectory.
     */
    private static File profileName( File profileFileDirectory, File mappedFile, long count )
    {
        String name = mappedFile.getName();
        return new File( profileFileDirectory, name + "." + count + SUFFIX_CACHEPROF );
    }

    static Predicate<Profile> relevantTo( PagedFile pagedFile )
    {
        return p -> p.pagedFile.equals( pagedFile.file() );
    }

    static Stream<Profile> parseProfileName( Path profilePath, Path mappedFilePath )
    {
        String name = profilePath.getFileName().toString();
        if ( !name.endsWith( SUFFIX_CACHEPROF ) )
        {
            return Stream.empty();
        }
        int lastDot = name.lastIndexOf( '.' );
        int secondLastDot = name.lastIndexOf( '.', lastDot - 1 );
        String countStr = name.substring( secondLastDot + 1, lastDot );
        try
        {
            long sequenceId = Long.parseLong( countStr, 10 );
            String targetMappedFileName = name.substring( 0, secondLastDot );
            if ( targetMappedFileName.equals( mappedFilePath.getFileName().toString() ) )
            {
                return Stream.of( new Profile( profilePath.toFile(), mappedFilePath.toFile(), sequenceId ) );
            }
            else
            {
                return Stream.empty();
            }
        }
        catch ( NumberFormatException e )
        {
            return Stream.empty();
        }
    }
}
