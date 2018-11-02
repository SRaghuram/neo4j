/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.storecopy.StoreWriter;

@Deprecated
public interface TheBackupInterface
{
    Response<Void> fullBackup( StoreWriter writer, boolean forensics );

    Response<Void> incrementalBackup( RequestContext context );
}
