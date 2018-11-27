/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management.impl;

import javax.management.NotCompliantMBeanException;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.Service;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.management.IndexSamplingManager;

import static org.neo4j.internal.kernel.api.TokenRead.NO_TOKEN;
import static org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode.TRIGGER_REBUILD_ALL;
import static org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode.TRIGGER_REBUILD_UPDATED;

@Service.Implementation( ManagementBeanProvider.class )
public final class IndexSamplingManagerBean extends ManagementBeanProvider
{
    public IndexSamplingManagerBean()
    {
        super( IndexSamplingManager.class );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new IndexSamplingManagerImpl( management );
    }

    @Override
    protected Neo4jMBean createMXBean( ManagementData management )
    {
        return new IndexSamplingManagerImpl( management, true );
    }

    private static class IndexSamplingManagerImpl extends Neo4jMBean implements IndexSamplingManager
    {
        private final StoreAccess access;

        IndexSamplingManagerImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );
            this.access = access( management );
        }

        IndexSamplingManagerImpl( ManagementData management, boolean mxBean )
        {
            super( management, mxBean );
            this.access = access( management );
        }

        @Override
        public void triggerIndexSampling( String labelKey, String propertyKey, boolean forceSample )
        {
            access.triggerIndexSampling( labelKey, propertyKey, forceSample );
        }
    }

    private static StoreAccess access( ManagementData management )
    {
        return new StoreAccess( management.getDatabase() );
    }

    static class StoreAccess
    {
        private final Database database;

        StoreAccess( Database database )
        {
            this.database = database;
        }

        void triggerIndexSampling( String labelKey, String propertyKey, boolean forceSample )
        {
            DependencyResolver dependencyResolver = database.getDependencyResolver();
            IndexingService indexingService = dependencyResolver.resolveDependency( IndexingService.class );
            TokenHolders tokenHolders = dependencyResolver.resolveDependency( TokenHolders.class );
            int labelKeyId = tokenHolders.labelTokens().getIdByName( labelKey );
            int propertyKeyId = tokenHolders.propertyKeyTokens().getIdByName( propertyKey );
            if ( labelKeyId == NO_TOKEN )
            {
                throw new IllegalArgumentException( "No label key was found associated with " + labelKey );
            }
            if ( propertyKeyId == NO_TOKEN )
            {
                throw new IllegalArgumentException( "No property was found associated with " + propertyKey );
            }
            try
            {
                indexingService.triggerIndexSampling( SchemaDescriptorFactory.forLabel( labelKeyId, propertyKeyId ), getIndexSamplingMode( forceSample ) );
            }
            catch ( IndexNotFoundKernelException e )
            {
                throw new IllegalArgumentException( e.getMessage() );
            }
        }

        private IndexSamplingMode getIndexSamplingMode( boolean forceSample )
        {
            return forceSample ? TRIGGER_REBUILD_ALL : TRIGGER_REBUILD_UPDATED;
        }
    }
}
