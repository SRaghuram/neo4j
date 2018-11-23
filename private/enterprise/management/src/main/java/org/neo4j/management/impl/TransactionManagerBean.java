/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management.impl;

import javax.management.NotCompliantMBeanException;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.management.TransactionManager;

@Service.Implementation( ManagementBeanProvider.class )
public final class TransactionManagerBean extends ManagementBeanProvider
{
    public TransactionManagerBean()
    {
        super( TransactionManager.class );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new TransactionManagerImpl( management );
    }

    private static class TransactionManagerImpl extends Neo4jMBean implements TransactionManager
    {
        private final DatabaseTransactionStats txMonitor;
        private final DatabaseManager databaseManager;

        TransactionManagerImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );
            this.txMonitor = management.resolveDependency( DatabaseTransactionStats.class );
            this.databaseManager = management.resolveDependency( DatabaseManager.class );
        }

        @Override
        public long getNumberOfOpenTransactions()
        {
            return txMonitor.getNumberOfActiveTransactions();
        }

        @Override
        public long getPeakNumberOfConcurrentTransactions()
        {
            return txMonitor.getPeakConcurrentNumberOfTransactions();
        }

        @Override
        public long getNumberOfOpenedTransactions()
        {
            return txMonitor.getNumberOfStartedTransactions();
        }

        @Override
        public long getNumberOfCommittedTransactions()
        {
            return txMonitor.getNumberOfCommittedTransactions();
        }

        @Override
        public long getNumberOfRolledBackTransactions()
        {
            return txMonitor.getNumberOfRolledBackTransactions();
        }

        @Override
        public long getLastCommittedTxId()
        {
            return databaseManager.getDatabaseContext( GraphDatabaseSettings.DEFAULT_DATABASE_NAME )
                                 .map( DatabaseContext::getDependencies )
                                 .map( resolver -> resolver.resolveDependency( TransactionIdStore.class ) )
                                 .map( TransactionIdStore::getLastCommittedTransactionId )
                                 .orElse( -1L );
        }
    }
}
