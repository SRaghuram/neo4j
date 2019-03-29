/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.storage;

import com.neo4j.causalclustering.core.state.CoreStateFiles;
import com.neo4j.causalclustering.core.state.StateRecoveryManager;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FlushableChannel;
import org.neo4j.io.fs.PhysicalFlushableChannel;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class DurableStateStorage<STATE> extends LifecycleAdapter implements StateStorage<STATE>
{
    private final StateRecoveryManager<STATE> recoveryManager;
    private final Log log;
    private STATE initialState;
    private final File fileA;
    private final File fileB;
    private final FileSystemAbstraction fsa;
    private final CoreStateFiles<STATE> fileType;
    private final StateMarshal<STATE> marshal;
    private final int numberOfEntriesBeforeRotation;

    private int numberOfEntriesWrittenInActiveFile;
    private File currentStoreFile;

    private PhysicalFlushableChannel currentStoreChannel;

    public DurableStateStorage( FileSystemAbstraction fsa, File baseDir, CoreStateFiles<STATE> fileType,
            int numberOfEntriesBeforeRotation, LogProvider logProvider )
    {
        this.fsa = fsa;
        this.fileType = fileType;
        this.marshal = fileType.marshal();
        this.numberOfEntriesBeforeRotation = numberOfEntriesBeforeRotation;
        this.log = logProvider.getLog( getClass() );
        this.recoveryManager = new StateRecoveryManager<>( fsa, this.marshal );
        this.fileA = new File( baseDir, fileType.name() + ".a" );
        this.fileB = new File( baseDir, fileType.name() + ".b" );
    }

    @Override
    public boolean exists()
    {
        return fsa.fileExists( fileA ) && fsa.fileExists( fileB );
    }

    private void create() throws IOException
    {
        ensureExists( fileA );
        ensureExists( fileB );
    }

    private void ensureExists( File file ) throws IOException
    {
        if ( !fsa.fileExists( file ) )
        {
            fsa.mkdirs( file.getParentFile() );
            try ( FlushableChannel channel = new PhysicalFlushableChannel( fsa.write( file ) ) )
            {
                marshal.marshal( marshal.startState(), channel );
            }
        }
    }

    private void recover() throws IOException
    {
        final StateRecoveryManager.RecoveryStatus<STATE> recoveryStatus = recoveryManager.recover( fileA, fileB );

        this.currentStoreFile = recoveryStatus.activeFile();
        this.currentStoreChannel = resetStoreFile( currentStoreFile );
        this.initialState = recoveryStatus.recoveredState();

        log.info( "%s state restored, up to ordinal %d", fileType, marshal.ordinal( initialState ) );
    }

    @Override
    public STATE getInitialState()
    {
        assert initialState != null;
        return initialState;
    }

    @Override
    public void init() throws IOException
    {
        create();
        recover();
    }

    @Override
    public synchronized void shutdown() throws IOException
    {
        currentStoreChannel.close();
        currentStoreChannel = null;
    }

    @Override
    public synchronized void persistStoreData( STATE state ) throws IOException
    {
        if ( numberOfEntriesWrittenInActiveFile >= numberOfEntriesBeforeRotation )
        {
            switchStoreFile();
            numberOfEntriesWrittenInActiveFile = 0;
        }

        marshal.marshal( state, currentStoreChannel );
        currentStoreChannel.prepareForFlush().flush();

        numberOfEntriesWrittenInActiveFile++;
    }

    @Override
    public String toString()
    {
        return "DurableStateStorage{" +
               "fileType=" + fileType +
               '}';
    }

    private void switchStoreFile() throws IOException
    {
        currentStoreChannel.close();

        if ( currentStoreFile.equals( fileA ) )
        {
            currentStoreChannel = resetStoreFile( fileB );
            currentStoreFile = fileB;
        }
        else
        {
            currentStoreChannel = resetStoreFile( fileA );
            currentStoreFile = fileA;
        }
    }

    private PhysicalFlushableChannel resetStoreFile( File nextStore ) throws IOException
    {
        fsa.truncate( nextStore, 0 );
        return new PhysicalFlushableChannel( fsa.write( nextStore ) );
    }

}
