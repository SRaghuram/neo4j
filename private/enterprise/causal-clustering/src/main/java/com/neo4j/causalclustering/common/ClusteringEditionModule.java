/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.kernel.impl.enterprise.CommercialConstraintSemantics;
import com.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import com.neo4j.kernel.impl.net.DefaultNetworkConnectionTracker;
import com.neo4j.kernel.impl.pagecache.PageCacheWarmer;

import java.util.function.Predicate;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.lifecycle.LifeSupport;

public abstract class ClusteringEditionModule extends AbstractEditionModule
{
    protected void editionInvariants( GlobalModule globalModule, Dependencies dependencies, Config config, LifeSupport life )
    {
        ioLimiter = new ConfigurableIOLimiter( globalModule.getGlobalConfig() );

        headerInformationFactory = createHeaderInformationFactory();

        constraintSemantics = new CommercialConstraintSemantics();

        connectionTracker = dependencies.satisfyDependency( createConnectionTracker() );
    }

    private static TransactionHeaderInformationFactory createHeaderInformationFactory()
    {
        return () -> new TransactionHeaderInformation( -1, -1, new byte[0] );
    }

    @Override
    protected NetworkConnectionTracker createConnectionTracker()
    {
        return new DefaultNetworkConnectionTracker();
    }

    public static Predicate<String> fileWatcherFileNameFilter()
    {
        return Predicates.any( fileName -> fileName.startsWith( TransactionLogFilesHelper.DEFAULT_NAME ),
                filename -> filename.endsWith( PageCacheWarmer.SUFFIX_CACHEPROF ) );
    }
}
