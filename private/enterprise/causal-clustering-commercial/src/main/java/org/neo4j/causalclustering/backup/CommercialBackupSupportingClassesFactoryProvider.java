/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.backup;

import org.neo4j.backup.AbstractBackupSupportingClassesFactory;
import org.neo4j.backup.BackupModuleResolveAtRuntime;
import org.neo4j.backup.BackupSupportingClassesFactoryProvider;

public class CommercialBackupSupportingClassesFactoryProvider extends BackupSupportingClassesFactoryProvider
{
    /**
     * No args constructor for Service discovery
     */
    public CommercialBackupSupportingClassesFactoryProvider()
    {
        super( null );
    }

    /**
     * Create a new instance of a service implementation identified with the
     * specified key(s).
     *
     * @param key the main key for identifying this service implementation
     * @param altKeys alternative spellings of the identifier of this service
     */
    protected CommercialBackupSupportingClassesFactoryProvider( String key, String... altKeys )
    {
        super( key, altKeys );
    }

    @Override
    public AbstractBackupSupportingClassesFactory getFactory( BackupModuleResolveAtRuntime backupModuleResolveAtRuntime )
    {
        return new CommercialBackupSupportingClassesFactory( backupModuleResolveAtRuntime );
    }

    @Override
    protected int getPriority()
    {
        return 1;
    }
}
