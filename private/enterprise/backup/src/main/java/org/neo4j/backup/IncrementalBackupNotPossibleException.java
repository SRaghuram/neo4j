/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup;

public class IncrementalBackupNotPossibleException extends RuntimeException
{
    public IncrementalBackupNotPossibleException( String msg, Throwable cause )
    {
        super( msg, cause );
    }

}
