/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.txlog;

import org.neo4j.kernel.impl.transaction.log.LogPosition;

/**
 * Handler of inconsistencies discovered by {@link CheckTxLogs} tool.
 */
interface InconsistenciesHandler
{
    /**
     * For reporting of invalid check points.
     * @param logVersion the log file version where the check point is located in
     * @param logPosition the invalid logPosition stored in the check point entry
     * @param size the size of file pointed by the check point entry
     */
    void reportInconsistentCheckPoint( long logVersion, LogPosition logPosition, long size );

    /**
     * For reporting of inconsistencies found between before and after state of commands.
     * @param committed the record seen previously during transaction log scan and considered valid
     * @param current the record met during transaction log scan and considered inconsistent with committed
     */
    void reportInconsistentCommand( RecordInfo<?> committed, RecordInfo<?> current );

    /**
     * For reporting of inconsistencies found about tx id sequences
     * @param lastSeenTxId last seen tx id before processing the current commit
     * @param currentTxId the transaction id of the process commit entry
     */
    void reportInconsistentTxIdSequence( long lastSeenTxId, long currentTxId );
}
