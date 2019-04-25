/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.stores;

import java.io.File;
import java.nio.file.Files;

import org.neo4j.io.layout.DatabaseLayout;

public class BackupStoreWithSomeDataAndNoIdFiles extends BackupStoreWithSomeData
{
    @Override
    void modify( File backup ) throws Exception
    {
        DatabaseLayout layout = DatabaseLayout.of( backup );
        for ( File idFile : layout.idFiles() )
        {
            Files.delete( idFile.toPath() );
        }
    }
}
