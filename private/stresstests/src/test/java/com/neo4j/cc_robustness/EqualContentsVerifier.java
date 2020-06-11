/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness;

import com.neo4j.causalclustering.core.state.CoreInstanceInfo;

import java.rmi.RemoteException;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.Predicates;
import org.neo4j.logging.Log;

class EqualContentsVerifier
{
    private final Log log;

    EqualContentsVerifier( Log log )
    {
        this.log = log;
    }

    boolean verifyEqualContents( Iterable<CcInstance> instances ) throws Exception
    {
        int timeout = 180;
        log.info( "Waiting %ds for core states to sync up to: %d", timeout, getMaxAppendIndex( instances ) );

        Predicates.awaitEx( () -> checkAllInSync( instances ), timeout, TimeUnit.SECONDS );

        String referenceChecksum = null;

        boolean allEqual = true;
        for ( CcInstance instance : instances )
        {
            if ( referenceChecksum == null )
            {
                InstanceInfo info = new InstanceInfo( instance );
                referenceChecksum = info.checksum;

                log.info( "%-11s%s", "Reference: ", info );
                log.info( "%-11s%s", "Reference: ", instance.coreInfo() );
                continue;
            }

            InstanceInfo info = new InstanceInfo( instance );
            CoreInstanceInfo coreInfo = instance.coreInfo();

            if ( info.checksum.equals( referenceChecksum ) )
            {
                log.info( "%-11s%s", "Equals: ", info );
                log.info( "%-11s%s", "Equals: ", coreInfo );
            }
            else
            {
                log.error( "%-11s%s", "Differs: ", info );
                log.error( "%-11s%s", "Differs: ", info );
                allEqual = false;
            }
        }

        return allEqual;
    }

    private boolean checkAllInSync( Iterable<CcInstance> instances )
    {
        long maxAppendIndex = getMaxAppendIndex( instances );

        for ( CcInstance instance : instances )
        {
            CoreInstanceInfo info = instance.coreInfo();
            if ( info.appliedIndex() != maxAppendIndex )
            {
                return false;
            }
        }
        return true;
    }

    private long getMaxAppendIndex( Iterable<CcInstance> instances )
    {
        long maxAppendIndex = -1;
        for ( CcInstance instance : instances )
        {
            CoreInstanceInfo info = instance.coreInfo();
            maxAppendIndex = Math.max( info.appendIndex(), maxAppendIndex );
        }
        return maxAppendIndex;
    }

    private static class InstanceInfo
    {
        final long instanceId;
        final long lastCommittedTxId;
        final long lastClosedTxId;
        final String checksum;

        private InstanceInfo( CcInstance instance ) throws RemoteException
        {
            this.lastCommittedTxId = instance.getLastCommittedTxId();
            this.lastClosedTxId = instance.getLastClosedTxId();
            this.instanceId = instance.getServerId();
            this.checksum = instance.storeChecksum();
        }

        @Override
        public String toString()
        {
            return "InstanceInfo{" + "instanceId=" + instanceId + ", lastCommittedTxId=" + lastCommittedTxId + ", lastClosedTxId=" + lastClosedTxId +
                    ", checksum='" + checksum + '\'' + '}';
        }
    }
}
