/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

class UnencryptedTransactionAndBackupPortsIT extends BaseEncryptedBackupIT
{
    UnencryptedTransactionAndBackupPortsIT()
    {
        super( false, false );
    }
}
