/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.junit.jupiter.api.Test;

import com.neo4j.causalclustering.handlers.PipelineWrapper;
import com.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import org.neo4j.kernel.configuration.Config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.backup.impl.BackupSupportingClassesFactoryProvider.getProvidersByPriority;

class OnlineBackupCommandProviderTest
{
    @Test
    void communityBackupSupportingFactory()
    {
        BackupModule backupModule = mock( BackupModule.class );

        BackupSupportingClassesFactoryProvider provider = getProvidersByPriority()
                .filter( p -> p.getClass().equals( BackupSupportingClassesFactoryProvider.class ) ).findFirst().get();
        BackupSupportingClassesFactory factory = provider.getFactory( backupModule );
        assertEquals( VoidPipelineWrapperFactory.VOID_WRAPPER,
                factory.createPipelineWrapper( Config.defaults() ) );
    }

    /**
     * This class must be public and static because it must be service loadable.
     */
    public static class DummyProvider extends BackupSupportingClassesFactoryProvider
    {
        @Override
        public BackupSupportingClassesFactory getFactory( BackupModule backupModule )
        {
            return new BackupSupportingClassesFactory( backupModule )
            {
                @Override
                protected PipelineWrapper createPipelineWrapper( Config config )
                {
                    throw new AssertionError( "This provider should never be loaded" );
                }
            };
        }

        @Override
        protected int getPriority()
        {
            return super.getPriority() - 1;
        }
    }
}
