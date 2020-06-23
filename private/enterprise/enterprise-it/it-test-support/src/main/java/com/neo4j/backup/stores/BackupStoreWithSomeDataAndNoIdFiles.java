/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.stores;

import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.io.layout.DatabaseLayout;

public class BackupStoreWithSomeDataAndNoIdFiles extends BackupStoreWithSomeData
{
    @Override
    void modify( Path backup ) throws Exception
    {
        DatabaseLayout layout = DatabaseLayout.ofFlat( backup );
        for ( Path idFile : layout.idFiles() )
        {
            Files.delete( idFile );
        }
    }
}
