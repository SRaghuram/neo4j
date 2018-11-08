/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.function.Predicate;

import org.neo4j.function.Predicates;
import org.neo4j.helper.IsChannelClosedException;
import org.neo4j.helper.IsConnectionException;
import org.neo4j.helper.IsConnectionResetByPeer;
import org.neo4j.helper.IsStoreClosed;

public class BackupHelper
{

    private static final Predicate<Throwable> isTransientError = Predicates.any(
                    new IsConnectionException(),
                    new IsConnectionResetByPeer(),
                    new IsChannelClosedException(),
                    new IsStoreClosed() );

    private BackupHelper()
    {
    }

    public static BackupResult backup( String host, int port, Path targetDirectory ) throws Exception
    {
        boolean consistent = true;
        boolean transientFailure = false;
        boolean failure = false;
        // This is where backup should happen
        return new BackupResult( consistent, transientFailure );
    }

    private static void flushToStandardOutput( ByteArrayOutputStream outputStream )
    {
        try
        {
            outputStream.writeTo( System.out );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
