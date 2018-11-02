/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.helpers.Service;

import static java.util.Comparator.comparingInt;

public class BackupSupportingClassesFactoryProvider extends Service
{
    /**
     * Create a new instance of a service implementation identified with the
     * specified key(s).
     *
     * @param key the main key for identifying this service implementation
     */
    public BackupSupportingClassesFactoryProvider( String key )
    {
        super( key );
    }

    public BackupSupportingClassesFactoryProvider()
    {
        super( "default-backup-support-provider" );
    }

    public BackupSupportingClassesFactory getFactory( BackupModule backupModule )
    {
        return new BackupSupportingClassesFactory( backupModule );
    }

    public static Stream<BackupSupportingClassesFactoryProvider> getProvidersByPriority()
    {
        Stream<BackupSupportingClassesFactoryProvider> providers = StreamSupport.stream(
                load( BackupSupportingClassesFactoryProvider.class ).spliterator(), false );
        // Inject the default provider into the stream, so it participates in sorting by priority.
        providers = Stream.concat( providers, Stream.of( new BackupSupportingClassesFactoryProvider() ) );
        return providers.sorted( comparingInt( BackupSupportingClassesFactoryProvider::getPriority ).reversed() );
    }

    /**
     * The higher the priority value, the greater the preference
     */
    protected int getPriority()
    {
        return 42;
    }
}
