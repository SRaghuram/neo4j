/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.com.storecopy;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.io.fs.FileSystemAbstraction;

import static com.neo4j.com.storecopy.FileMoveAction.moveViaFileSystem;
import static java.util.stream.Collectors.toList;

public class FileMoveProvider
{
    private final FileSystemAbstraction fs;

    public FileMoveProvider( FileSystemAbstraction fs )
    {
        this.fs = fs;
    }

    /**
     * Construct a stream of files that are to be moved
     *
     * @param dir the source location of the move action
     * @return a stream of the entire contents of the source location that can be applied to a target location to
     * perform a move
     */
    public Stream<FileMoveAction> traverseForMoving( Path dir )
    {
        return traverseForMoving( dir, dir );
    }

    /**
     * Copies <b>the contents</b> from the directory to the base target path.
     * <p>
     * This is confusing, so here is an example
     * <p>
     * <p>
     * <code>
     * +Parent<br>
     * |+--directoryA<br>
     * |...+--fileA<br>
     * |...+--fileB<br>
     * </code>
     * <p>
     * Suppose we want to move to move <b>Parent/directoryA</b> to <b>Parent/directoryB</b>.<br>
     * <p>
     * <code>
     * File directoryA = new File("Parent/directoryA");<br>
     * Stream<FileMoveAction> fileMoveActions = new FileMoveProvider(pageCache).traverseGenerateMoveActions
     * (directoryA, directoryA);<br>
     * </code>
     * </p>
     * In the above we clearly generate actions for moving all the files contained in directoryA. directoryA is
     * mentioned twice due to a implementation detail,
     * hence the public method with only one parameter. We then actually perform the moves by applying the base
     * target directory that we want to move to.
     * <p>
     * <code>
     * File directoryB = new File("Parent/directoryB");<br>
     * fileMoveActions.forEach( action -> action.move( directoryB ) );
     * </code>
     * </p>
     *
     * @param dir this directory and all the child paths under it are subject to move
     * @param basePath this is the parent of your intended target directory.
     * @return a stream of individual move actions which can be iterated and applied whenever
     */
    private Stream<FileMoveAction> traverseForMoving( Path dir, Path basePath )
    {
        // Note that flatMap is an *intermediate operation* and therefor always lazy.
        // It is very important that the stream we return only *lazily* calls out to expandTraverseFiles!
        return Stream.of( dir ).flatMap( d -> expandTraverseFiles( d, basePath ) );
    }

    private Stream<FileMoveAction> expandTraverseFiles( Path dir, Path basePath )
    {
        List<Path> listing = listFiles( dir );
        if ( listing == null )
        {
            // This happens if what we were given as 'dir' is not actually a directory, but a single specific file.
            // In that case, we will produce a stream of a single FileMoveAction for that file.
            listing = Collections.singletonList( dir );
            // This also means that the base path is currently the same as the file itself, which is wrong.
            // We change the base path to be the parent directory of the file, so that we can relativise the filename
            // correctly later.
            basePath = dir.getParent();
        }
        Path base = basePath; // Capture effectively-final base path snapshot.
        Stream<Path> files = listing.stream().filter( this::isFile );
        Stream<Path> dirs = listing.stream().filter( this::isDirectory );
        Stream<FileMoveAction> moveFiles = files.map( f -> moveViaFileSystem( f, base ) );
        Stream<FileMoveAction> traverseDirectories = dirs.flatMap( d -> traverseForMoving( d, base ) );
        return Stream.concat( moveFiles, traverseDirectories );
    }

    private boolean isFile( Path file )
    {
        return !fs.isDirectory( file );
    }

    private boolean isDirectory( Path file )
    {
        return fs.isDirectory( file );
    }

    private List<Path> listFiles( Path dir )
    {
        Path[] fsaFiles = fs.listFiles( dir );
        if ( fsaFiles == null )
        {
            // This probably means 'dir' is actually a file, or it does not exist.
            return null;
        }

        return Arrays.stream( fsaFiles ).distinct().collect( toList() );
    }
}
