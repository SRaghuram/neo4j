/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.lock;

import org.neo4j.com.ComException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.kernel.ha.com.master.Master;

/**
 * Thrown upon network communication failures, when taking or releasing distributed locks in HA.
 */
public class DistributedLockFailureException extends TransientTransactionFailureException
{
    public DistributedLockFailureException( String message, Master master, ComException cause )
    {
        super( message + " (for master instance " + master + "). The most common causes of this exception are " +
               "network failures, or master-switches where the failing transaction was started before the last " +
               "master election.", cause );
    }
}
