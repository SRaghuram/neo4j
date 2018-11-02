/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.lock;

import java.io.StringWriter;

import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.locking.DumpLocksVisitor;
import org.neo4j.kernel.impl.locking.LockType;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.logging.FormattedLog;
import org.neo4j.storageengine.api.lock.ResourceType;

import static java.lang.String.format;

/**
 * Temporary exception to aid in driving out a nasty "lock get stuck" issue in HA. Since it's subclasses
 * {@link DeadlockDetectedException} it will be invisible to users and code that already handle such
 * deadlock exceptions and retry. This exception is thrown instead of awaiting a lock locally on a slave
 * after it was acquired on the master, since applying a lock locally after master granted it should succeed,
 * or fail; it cannot wait for another condition.
 *
 * While this work-around is in place there is more breathing room to figure out the real problem preventing
 * some local locks to be grabbed.
 *
 * @author Mattias Persson
 */
public class LocalDeadlockDetectedException extends DeadlockDetectedException
{
    public LocalDeadlockDetectedException( Locks.Client lockClient, Locks lockManager, ResourceType resourceType, long resourceId,
            LockType type )
    {
        super( constructHelpfulDiagnosticsMessage( lockClient, lockManager, resourceType, resourceId, type ) );
    }

    private static String constructHelpfulDiagnosticsMessage( Locks.Client client, Locks lockManager,
                                                  ResourceType resourceType, long resourceId, LockType type )
    {
        StringWriter stringWriter = new StringWriter();
        stringWriter.append( format(
                "%s tried to apply local %s lock on %s(%s) after acquired on master. Currently these locks exist:%n",
                client, type, resourceType, resourceId ) );

        lockManager.accept( new DumpLocksVisitor( FormattedLog.withUTCTimeZone().toWriter( stringWriter ) ) );
        return stringWriter.toString();
    }
}
