/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness;

import com.neo4j.causalclustering.core.state.CoreInstanceInfo;
import com.neo4j.cc_robustness.workload.GraphOperations;
import com.neo4j.cc_robustness.workload.SchemaOperation;
import com.neo4j.cc_robustness.workload.ShutdownType;
import com.neo4j.cc_robustness.workload.Work;

import java.rmi.Remote;
import java.rmi.RemoteException;

import org.neo4j.test.DbRepresentation;

public interface CcInstance extends Remote
{
    int NF_OUT = 1;
    int NF_IN = 2;

    void awaitStarted() throws RemoteException;

    int getServerId() throws RemoteException;

    void doWorkOnDatabase( Work work ) throws RemoteException;

    void doBatchOfOperations( Integer txSize, GraphOperations.Operation... operationOrNoneForRandom ) throws RemoteException;

    void doSchemaOperation( SchemaOperation... operationOrNoneForRandom ) throws RemoteException;

    void shutdown( ShutdownType type ) throws RemoteException;

    boolean isLeader() throws RemoteException;

    CoreInstanceInfo coreInfo();

    void dumpLocks() throws RemoteException;

    void verifyConsistencyOffline() throws RemoteException;

    String storeChecksum() throws RemoteException;

    DbRepresentation representation() throws RemoteException;

    long getLastCommittedTxId() throws RemoteException;

    long getLastClosedTxId() throws RemoteException;

    void rotateLogs() throws RemoteException;

    int getNumberOfBranches() throws RemoteException;

    void createReferenceNode() throws RemoteException;

    /**
     * @param flags which network cables to block. As boxed {@link Integer} to work-around a shortcoming
     * in {@link SubProcess}.
     * @return {@code true} if network was actually blocked, i.e. if it is not already blocked by someone else.
     */
    boolean blockNetwork( Integer flags ) throws RemoteException;

    /**
     * Restores damage made by {@link #blockNetwork(Integer)}.
     */
    void restoreNetwork() throws RemoteException;

    boolean isAvailable() throws RemoteException;

    void log( String message ) throws RemoteException;

    void slowDownLogging( Float probability ) throws RemoteException;
}
