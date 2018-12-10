/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.storage;

import com.neo4j.causalclustering.core.state.CoreStateFiles;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.logging.LogProvider;

public class DurableStateStorageImporter<STATE> extends DurableStateStorage<STATE>
{
    public DurableStateStorageImporter( FileSystemAbstraction fileSystemAbstraction, File stateDir, CoreStateFiles fileType,
                                        StateMarshal<STATE> marshal, int numberOfEntriesBeforeRotation,
                                        Supplier<DatabaseHealth> databaseHealthSupplier, LogProvider logProvider )
    {
        super( fileSystemAbstraction, stateDir, fileType, numberOfEntriesBeforeRotation, logProvider );
    }

    public void persist( STATE state ) throws IOException
    {
        super.persistStoreData( state );
        super.switchStoreFile();
        super.persistStoreData( state );
    }
}
