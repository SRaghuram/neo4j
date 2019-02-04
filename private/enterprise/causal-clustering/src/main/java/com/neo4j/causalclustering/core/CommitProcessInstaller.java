/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import java.util.Optional;

import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.storageengine.api.StorageEngine;

public class CommitProcessInstaller
{
    private InstallListener installListener;
    private volatile TransactionRepresentationCommitProcess commitProcess;

    public synchronized void install( TransactionAppender transactionAppender, StorageEngine storageEngine )
    {
        if ( transactionAppender == null || storageEngine == null )
        {
            throw new NullPointerException( "Commit process dependencies may not be null!" );
        }

        this.commitProcess = new TransactionRepresentationCommitProcess( transactionAppender, storageEngine );
        updateListener();
    }

    synchronized void registerInstallListener( InstallListener installListener )
    {
        if ( this.installListener != null )
        {
            throw new IllegalStateException( "Attempted to register multiple listeners to the same commitProcess installer!" );
        }
        this.installListener = installListener;
        updateListener();
    }

    public Optional<TransactionRepresentationCommitProcess> installed()
    {
        return Optional.ofNullable( commitProcess );
    }

    private void updateListener()
    {
        if ( installListener != null && commitProcess != null )
        {
            installListener.install( commitProcess );
        }
    }

    @FunctionalInterface
    interface InstallListener
    {
        void install( TransactionRepresentationCommitProcess commitProcess );
    }
}
