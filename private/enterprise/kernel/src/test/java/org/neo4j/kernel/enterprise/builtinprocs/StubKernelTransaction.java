/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.enterprise.builtinprocs;

import org.mockito.Answers;

import java.util.Map;
import java.util.Optional;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.ExecutionStatistics;
import org.neo4j.internal.kernel.api.Locks;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.Token;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.ClockContext;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.IndexDescriptor;
import org.neo4j.storageengine.api.schema.SchemaDescriptor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;

class StubKernelTransaction implements KernelTransaction
{
    @Override
    public Statement acquireStatement()
    {
        return null;
    }

    @Override
    public IndexDescriptor indexUniqueCreate( SchemaDescriptor schema, String provider )
    {
        return null;
    }

    @Override
    public void success()
    {
    }

    @Override
    public void failure()
    {
    }

    @Override
    public Read dataRead()
    {
        return null;
    }

    @Override
    public Write dataWrite()
    {
        return null;
    }

    @Override
    public TokenRead tokenRead()
    {
        return null;
    }

    @Override
    public TokenWrite tokenWrite()
    {
        return null;
    }

    @Override
    public Token token()
    {
        return null;
    }

    @Override
    public SchemaRead schemaRead()
    {
        return null;
    }

    @Override
    public Procedures procedures()
    {
        return null;
    }

    @Override
    public ExecutionStatistics executionStatistics()
    {
        return null;
    }

    @Override
    public SchemaWrite schemaWrite()
    {
        return null;
    }

    @Override
    public Locks locks()
    {
        return null;
    }

    @Override
    public CursorFactory cursors()
    {
        return null;
    }

    @Override
    public long closeTransaction()
    {
        return 0;
    }

    @Override
    public boolean isOpen()
    {
        return false;
    }

    @Override
    public SecurityContext securityContext()
    {
        SecurityContext securityContext = mock( SecurityContext.class, Answers.RETURNS_DEEP_STUBS );
        when( securityContext.subject().username() ).thenReturn( "testUser" );
        return securityContext;
    }

    @Override
    public ClientConnectionInfo clientInfo()
    {
        return EMBEDDED_CONNECTION;
    }

    @Override
    public AuthSubject subjectOrAnonymous()
    {
        AuthSubject subject = mock( AuthSubject.class );
        when( subject.username() ).thenReturn( "testUser" );
        return subject;
    }

    @Override
    public Optional<Status> getReasonIfTerminated()
    {
        return Optional.empty();
    }

    @Override
    public boolean isTerminated()
    {
        return false;
    }

    @Override
    public void markForTermination( Status reason )
    {
    }

    @Override
    public long lastTransactionTimestampWhenStarted()
    {
        return 0;
    }

    @Override
    public long lastTransactionIdWhenStarted()
    {
        return 0;
    }

    @Override
    public long startTime()
    {
        return 1984;
    }

    @Override
    public long timeout()
    {
        return 0;
    }

    @Override
    public void registerCloseListener( CloseListener listener )
    {
    }

    @Override
    public Type transactionType()
    {
        return null;
    }

    @Override
    public long getTransactionId()
    {
        return 8;
    }

    @Override
    public long getCommitTime()
    {
        return 0;
    }

    @Override
    public Revertable overrideWith( SecurityContext context )
    {
        return null;
    }

    @Override
    public NodeCursor ambientNodeCursor()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public RelationshipScanCursor ambientRelationshipCursor()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public PropertyCursor ambientPropertyCursor()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void setMetaData( Map<String,Object> metaData )
    {
    }

    @Override
    public ClockContext clocks()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void assertOpen()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }
}
