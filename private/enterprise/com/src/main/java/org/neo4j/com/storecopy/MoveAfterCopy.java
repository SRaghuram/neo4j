/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com.storecopy;

import java.io.File;
import java.nio.file.StandardCopyOption;
import java.util.function.Function;
import java.util.stream.Stream;

@FunctionalInterface
public interface MoveAfterCopy
{
    void move( Stream<FileMoveAction> moves, File fromDirectory, Function<File, File> destinationFunction ) throws
            Exception;

    static MoveAfterCopy moveReplaceExisting()
    {
        return ( moves, fromDirectory, destinationFunction ) ->
        {
            Iterable<FileMoveAction> itr = moves::iterator;
            for ( FileMoveAction move : itr )
            {
                move.move( destinationFunction.apply( move.file() ), StandardCopyOption.REPLACE_EXISTING );
            }
        };
    }
}
