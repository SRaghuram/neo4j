/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.neo4j.helpers.Service;

@Service.Implementation( BackupSupportingClassesFactoryProvider.class )
public class CommercialBackupSupportingClassesFactoryProvider extends BackupSupportingClassesFactoryProvider
{
    /**
     * Constructor must be public for Service discovery.
     */
    public CommercialBackupSupportingClassesFactoryProvider()
    {
        super( "commercial-backup-support-provider" );
    }

    @Override
    public BackupSupportingClassesFactory getFactory( BackupModule backupModule )
    {
        return new CommercialBackupSupportingClassesFactory( backupModule );
    }

    @Override
    protected int getPriority()
    {
        return super.getPriority() + 100;
    }
}
