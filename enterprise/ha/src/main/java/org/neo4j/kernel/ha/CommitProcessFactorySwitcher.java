/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha;

import java.net.URI;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.ha.cluster.AbstractModeSwitcher;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.transaction.TransactionPropagator;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.CommitProcessFactory;
import org.neo4j.kernel.impl.transaction.KernelHealth;
import org.neo4j.kernel.impl.transaction.xaframework.LogicalTransactionStore;

public class CommitProcessFactorySwitcher extends AbstractModeSwitcher<CommitProcessFactory>
{
    private final TransactionPropagator pusher;
    private final Master master;
    private final RequestContextFactory requestContextFactory;
    private final DependencyResolver resolver;

    public CommitProcessFactorySwitcher( TransactionPropagator pusher,
                                         Master master,
                                         DelegateInvocationHandler<CommitProcessFactory> delegate,
                                         RequestContextFactory requestContextFactory,
                                         HighAvailabilityMemberStateMachine memberStateMachine,
                                         DependencyResolver resolver )
    {
        super( memberStateMachine, delegate );
        this.pusher = pusher;
        this.master = master;
        this.requestContextFactory = requestContextFactory;
        this.resolver = resolver;
    }

    @Override
    protected CommitProcessFactory getSlaveImpl( URI serverHaUri )
    {
        return new CommitProcessFactory()
        {
            @Override
            public TransactionCommitProcess create( LogicalTransactionStore logicalTransactionStore,
                                                    KernelHealth kernelHealth, NeoStore neoStore,
                                                    TransactionRepresentationStoreApplier storeApplier,
                                                    boolean recovery )
            {
                SlaveTransactionCommitProcess slaveTransactionCommitProcess = new SlaveTransactionCommitProcess(
                        master, requestContextFactory, logicalTransactionStore,
                        kernelHealth, neoStore, storeApplier, resolver, false );
                return slaveTransactionCommitProcess;
            }
        };
    }

    @Override
    protected CommitProcessFactory getMasterImpl()
    {
        return new CommitProcessFactory()
        {
            @Override
            public TransactionCommitProcess create( LogicalTransactionStore logicalTransactionStore,
                                                    KernelHealth kernelHealth, NeoStore neoStore,
                                                    TransactionRepresentationStoreApplier storeApplier,
                                                    boolean recovery )
            {
                return new MasterTransactionCommitProcess( logicalTransactionStore, kernelHealth, neoStore, storeApplier,
                        pusher );
            }
        };
    }
}
