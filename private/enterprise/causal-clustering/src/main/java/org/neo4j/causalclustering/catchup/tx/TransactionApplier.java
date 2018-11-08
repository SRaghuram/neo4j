/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.tx;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.storageengine.api.StorageEngine;

import static org.neo4j.kernel.impl.transaction.tracing.CommitEvent.NULL;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

public class TransactionApplier
{
    private final TransactionRepresentationCommitProcess commitProcess;
    private final VersionContextSupplier versionContextSupplier;

    public TransactionApplier( DependencyResolver resolver )
    {
        commitProcess = new TransactionRepresentationCommitProcess(
                resolver.resolveDependency( TransactionAppender.class ),
                resolver.resolveDependency( StorageEngine.class ) );
        versionContextSupplier = resolver.resolveDependency( VersionContextSupplier.class );
    }

    public void appendToLogAndApplyToStore( CommittedTransactionRepresentation tx ) throws TransactionFailureException
    {
        commitProcess.commit( new TransactionToApply( tx.getTransactionRepresentation(),
                tx.getCommitEntry().getTxId(), versionContextSupplier.getVersionContext() ), NULL, EXTERNAL );
    }
}
