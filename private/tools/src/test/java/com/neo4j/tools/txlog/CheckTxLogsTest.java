/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.txlog;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.LongFunction;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.internal.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.kernel.database.DbmsLogEntryWriterFactory;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.PositionAwarePhysicalFlushableChecksumChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;

import static com.neo4j.tools.txlog.checktypes.CheckTypes.CHECK_TYPES;
import static com.neo4j.tools.txlog.checktypes.CheckTypes.NODE;
import static com.neo4j.tools.txlog.checktypes.CheckTypes.PROPERTY;
import static com.neo4j.tools.txlog.checktypes.CheckTypes.RELATIONSHIP;
import static com.neo4j.tools.txlog.checktypes.CheckTypes.RELATIONSHIP_GROUP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.kernel.api.security.AuthSubject.AUTH_DISABLED;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryParserSetVersion.LogEntryV4_0;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.writeLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.files.ChannelNativeAccessor.EMPTY_ACCESSOR;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;

@EphemeralNeo4jLayoutExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class CheckTxLogsTest
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    void shouldReportNoInconsistenciesFromValidLog() throws Exception
    {
        // Given
        Path log = logFile( 1 );

        writeTxContent( log, 1,
                new Command.NodeCommand(
                        nodeRecord( 42, false, false, -1, -1, 1 ),
                        nodeRecord( 42, true, false, 42, -1, 1 )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 5, false, -1, -1 ),
                        propertyRecord( 5, true, -1, -1, 777 )
                ),
                new Command.NodeCommand(
                        nodeRecord( 1, true, true, 2, -1, 1 ),
                        nodeRecord( 1, true, false, -1, -1, 1 )
                )
        );

        writeTxContent( log, 2,
                new Command.NodeCommand(
                        nodeRecord( 2, false, false, -1, -1, 1 ),
                        nodeRecord( 2, true, false, -1, -1, 1 )
                ),
                new Command.NodeCommand(
                        nodeRecord( 42, true, false, 42, -1, 1 ),
                        nodeRecord( 42, true, false, 24, 5, 1 )
                )
        );
        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( System.out, fs );

        // When
        boolean success = checker.scan( getLogFiles(), handler, List.of( NODE ) );

        // Then
        assertTrue( success );

        assertEquals( 0, handler.recordInconsistencies.size() );
    }

    private LogFiles getLogFiles() throws IOException
    {
        return LogFilesBuilder.logFilesBasedOnlyBuilder( databaseLayout.databaseDirectory(), fs )
                              .withLogVersionRepository( new SimpleLogVersionRepository() )
                              .withTransactionIdStore( new SimpleTransactionIdStore() )
                              .withCommandReaderFactory( RecordStorageCommandReaderFactory.INSTANCE )
                              .withStoreId( StoreId.UNKNOWN )
                              .build();
    }

    @Test
    void shouldReportNodeInconsistenciesFromSingleLog() throws IOException
    {
        // Given
        Path log = logFile( 1 );

        writeTxContent( log, 0,
                new Command.NodeCommand(
                        nodeRecord( 42, false, false, -1, -1, 1 ),
                        nodeRecord( 42, true, false, 42, -1, 1 )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 5, false, -1, -1 ),
                        propertyRecord( 5, true, -1, -1, 777 )
                ),
                new Command.NodeCommand(
                        nodeRecord( 1, true, true, 2, -1, 1 ),
                        nodeRecord( 1, true, false, -1, -1, 1 )
                )
        );

        writeTxContent( log, 0,
                new Command.NodeCommand(
                        nodeRecord( 2, false, false, -1, -1, 1 ),
                        nodeRecord( 2, true, false, -1, -1, 1 )
                ),
                new Command.NodeCommand(
                        nodeRecord( 42, true, false, 24, -1, 1 ),
                        nodeRecord( 42, true, false, 24, 5, 1 )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( System.out, fs );

        // When
        boolean success = checker.scan( getLogFiles(), handler, List.of( NODE ) );

        // Then
        assertFalse( success );
        assertEquals( 1, handler.recordInconsistencies.size() );

        NodeRecord seenRecord = (NodeRecord) handler.recordInconsistencies.get( 0 ).committed.record();
        NodeRecord currentRecord = (NodeRecord) handler.recordInconsistencies.get( 0 ).current.record();

        assertEquals( 42, seenRecord.getId() );
        assertEquals( 42, seenRecord.getNextRel() );
        assertEquals( 42, currentRecord.getId() );
        assertEquals( 24, currentRecord.getNextRel() );
    }

    @Test
    void shouldReportTransactionIdAndInconsistencyCount() throws IOException
    {
        // Given
        Path log = logFile( 1 );

        writeTxContent( log, 0,
                new Command.NodeCommand(
                        nodeRecord( 42, false, false, -1, -1, 1 ),
                        nodeRecord( 42, true, false, 42, -1, 1 )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 5, false, -1, -1 ),
                        propertyRecord( 5, true, -1, -1, 777 )
                ),
                new Command.NodeCommand(
                        nodeRecord( 1, true, true, 2, -1, 1 ),
                        nodeRecord( 1, true, false, -1, -1, 1 )
                ),
                new Command.NodeCommand(
                        nodeRecord( 5, true, true, 2, -1, 1 ),
                        nodeRecord( 5, true, false, -1, -1, 1 )
                )

        );

        writeTxContent( log, 1,
                new Command.NodeCommand(
                        nodeRecord( 2, false, false, -1, -1, 1 ),
                        nodeRecord( 2, true, false, -1, -1, 1 )
                ),
                new Command.NodeCommand(
                        nodeRecord( 5, true, true, 2, -1, 1 ), // inconsistent
                        nodeRecord( 5, true, false, -1, -1, 1 )
                ),
                new Command.NodeCommand(
                        nodeRecord( 1, true, false, -1, -1, 1 ),
                        nodeRecord( 1, true, true, 2, 1, 1 )
                ),
                new Command.NodeCommand(
                        nodeRecord( 42, true, false, 24, -1, 1 ), // inconsistent
                        nodeRecord( 42, true, false, 24, 5, 1 )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( System.out, fs );

        // When
        checker.scan( getLogFiles(), handler, List.of( NODE ) );

        // Then
        assertEquals( 2, handler.recordInconsistencies.size() );

        assertEquals( 0, handler.recordInconsistencies.get( 0 ).committed.txId() );
        assertEquals( 1, handler.recordInconsistencies.get( 0 ).current.txId() );

        assertEquals( 0, handler.recordInconsistencies.get( 1 ).committed.txId() );
        assertEquals( 1, handler.recordInconsistencies.get( 1 ).current.txId() );
    }

    @Test
    void shouldReportNodeInconsistenciesFromDifferentLogs() throws IOException
    {
        // Given
        Path log1 = logFile( 1 );
        Path log2 = logFile( 2 );
        Path log3 = logFile( 3 );

        writeTxContent( log1, 0,
                new Command.NodeCommand(
                        nodeRecord( 42, false, false, -1, -1, 1 ),
                        nodeRecord( 42, true, false, 42, -1, 1 )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 5, true, -1, -1, 777 ),
                        propertyRecord( 5, true, -1, -1, 777, 888 )
                ),
                new Command.NodeCommand(
                        nodeRecord( 1, true, true, 2, -1, 1 ),
                        nodeRecord( 1, true, false, -1, -1, 1 )
                )
        );

        writeTxContent( log2, 0,
                new Command.NodeCommand(
                        nodeRecord( 2, false, false, -1, -1, 1 ),
                        nodeRecord( 2, true, false, -1, -1, 1 )
                )
        );

        writeTxContent( log3, 0,
                new Command.NodeCommand(
                        nodeRecord( 42, true, true, 42, -1, 1 ),
                        nodeRecord( 42, true, true, 42, 10, 1 )
                ),
                new Command.NodeCommand(
                        nodeRecord( 2, true, false, -1, -1, 5 ),
                        nodeRecord( 2, false, false, -1, -1, 5 )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( System.out, fs );

        // When
        boolean success = checker.scan( getLogFiles(), handler, List.of( NODE ) );

        // Then
        assertFalse( success );
        assertEquals( 2, handler.recordInconsistencies.size() );

        NodeRecord seenRecord1 = (NodeRecord) handler.recordInconsistencies.get( 0 ).committed.record();
        NodeRecord currentRecord1 = (NodeRecord) handler.recordInconsistencies.get( 0 ).current.record();

        assertEquals( 42, seenRecord1.getId() );
        assertFalse( seenRecord1.isDense() );
        assertEquals( 42, currentRecord1.getId() );
        assertTrue( currentRecord1.isDense() );

        NodeRecord seenRecord2 = (NodeRecord) handler.recordInconsistencies.get( 1 ).committed.record();
        NodeRecord currentRecord2 = (NodeRecord) handler.recordInconsistencies.get( 1 ).current.record();

        assertEquals( 2, seenRecord2.getId() );
        assertEquals( 1, seenRecord2.getLabelField() );
        assertEquals( 2, currentRecord2.getId() );
        assertEquals( 5, currentRecord2.getLabelField() );
    }

    @Test
    void shouldReportPropertyInconsistenciesFromSingleLog() throws IOException
    {
        // Given
        Path log = logFile( 1 );

        writeTxContent( log, 0,
                new Command.PropertyCommand(
                        propertyRecord( 42, false, -1, -1 ),
                        propertyRecord( 42, true, -1, -1, 10 )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 42, true, -1, -1, 10 ),
                        propertyRecord( 42, true, 24, -1, 10 )
                )
        );

        writeTxContent( log, 0,
                new Command.NodeCommand(
                        nodeRecord( 2, false, false, -1, -1, 1 ),
                        nodeRecord( 2, true, false, -1, -1, 1 )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 42, true, -1, -1, 10 ),
                        propertyRecord( 42, true, -1, -1, 10, 20 )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( System.out, fs );

        // When
        boolean success = checker.scan( getLogFiles(), handler, List.of( PROPERTY ) );

        // Then
        assertFalse( success );
        assertEquals( 1, handler.recordInconsistencies.size() );

        PropertyRecord seenRecord = (PropertyRecord) handler.recordInconsistencies.get( 0 ).committed.record();
        PropertyRecord currentRecord = (PropertyRecord) handler.recordInconsistencies.get( 0 ).current.record();

        assertEquals( 42, seenRecord.getId() );
        assertEquals( 24, seenRecord.getPrevProp() );
        assertEquals( 42, currentRecord.getId() );
        assertEquals( -1, currentRecord.getPrevProp() );
    }

    @Test
    void shouldReportPropertyInconsistenciesFromDifferentLogs() throws IOException
    {
        // Given
        Path log1 = logFile( 1 );
        Path log2 = logFile( 2 );
        Path log3 = logFile( 3 );

        writeTxContent( log1, 0,
                new Command.NodeCommand(
                        nodeRecord( 42, false, false, -1, -1, 1 ),
                        nodeRecord( 42, true, false, 42, -1, 1 )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 5, true, -1, -1, 777 ),
                        propertyRecord( 5, true, -1, -1, 777 )
                ),
                new Command.NodeCommand(
                        nodeRecord( 1, true, true, 2, -1, 1 ),
                        nodeRecord( 1, true, false, -1, -1, 1 )
                )
        );

        writeTxContent( log2, 0,
                new Command.PropertyCommand(
                        propertyRecord( 24, false, -1, -1 ),
                        propertyRecord( 24, true, -1, -1, 777 )
                )
        );

        writeTxContent( log3, 0,
                new Command.PropertyCommand(
                        propertyRecord( 24, false, -1, -1 ),
                        propertyRecord( 24, true, -1, -1, 777 )
                ),
                new Command.NodeCommand(
                        nodeRecord( 42, true, true, 42, -1, 1 ),
                        nodeRecord( 42, true, true, 42, 10, 1 )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 5, true, -1, -1, 777, 888 ),
                        propertyRecord( 5, true, -1, 9, 777, 888, 999 )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( System.out, fs );

        // When
        boolean success = checker.scan( getLogFiles(), handler, List.of( PROPERTY ) );

        // Then
        assertFalse( success );
        assertEquals( 2, handler.recordInconsistencies.size() );

        RecordInconsistency inconsistency1 = handler.recordInconsistencies.get( 0 );
        PropertyRecord seenRecord1 = (PropertyRecord) inconsistency1.committed.record();
        PropertyRecord currentRecord1 = (PropertyRecord) inconsistency1.current.record();

        assertEquals( 24, seenRecord1.getId() );
        assertTrue( seenRecord1.inUse() );
        assertEquals( 24, currentRecord1.getId() );
        assertFalse( currentRecord1.inUse() );
        assertEquals( 2, inconsistency1.committed.logVersion() );
        assertEquals( 3, inconsistency1.current.logVersion() );

        RecordInconsistency inconsistency2 = handler.recordInconsistencies.get( 1 );
        PropertyRecord seenRecord2 = (PropertyRecord) inconsistency2.committed.record();
        PropertyRecord currentRecord2 = (PropertyRecord) inconsistency2.current.record();

        assertEquals( 5, seenRecord2.getId() );
        assertEquals( 777, seenRecord2.getPropertyBlock( 0 ).getSingleValueInt() );
        assertEquals( 5, currentRecord2.getId() );
        assertEquals( 777, currentRecord2.getPropertyBlock( 0 ).getSingleValueInt() );
        assertEquals( 888, currentRecord2.getPropertyBlock( 1 ).getSingleValueInt() );
        assertEquals( 1, inconsistency2.committed.logVersion() );
        assertEquals( 3, inconsistency2.current.logVersion() );
    }

    @Test
    void shouldReportRelationshipInconsistenciesFromSingleLog() throws IOException
    {
        // Given
        Path log = logFile( 1 );

        writeTxContent( log, 0,
                new Command.RelationshipCommand(
                        createRelationshipRecord( 42, false, -1, -1, -1, -1, -1, -1, -1, false, false ),
                        createRelationshipRecord( 42, true, 1, 2, 3, 4, 5, 6, 7, true, true )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 5, false, -1, -1 ),
                        propertyRecord( 5, true, -1, -1, 777 )
                ),
                new Command.RelationshipCommand(
                        createRelationshipRecord( 21, true, 1, 2, 3, 4, 5, 6, 7, true, true ),
                        createRelationshipRecord( 21, false, -1, -1, -1, -1, -1, -1, -1, false, false )
                )
        );

        writeTxContent( log, 0,
                new Command.RelationshipCommand(
                        createRelationshipRecord( 53, true, 1, 2, 3, 4, 5, 6, 7, true, true ),
                        createRelationshipRecord( 53, true, 1, 2, 30, 4, 14, 6, 7, true, true )
                ),
                new Command.RelationshipCommand(
                        createRelationshipRecord( 42, true, 1, 2, 3, 9, 5, 6, 7, true, true ),
                        createRelationshipRecord( 42, true, 1, 2, 3, 4, 5, 6, 7, true, true )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( System.out, fs );

        // When
        checker.scan( getLogFiles(), handler, List.of( RELATIONSHIP ) );

        // Then
        assertEquals( 1, handler.recordInconsistencies.size() );

        RelationshipRecord seenRecord = (RelationshipRecord) handler.recordInconsistencies.get( 0 ).committed.record();
        RelationshipRecord currentRecord = (RelationshipRecord) handler.recordInconsistencies.get( 0 ).current.record();

        assertEquals( 42, seenRecord.getId() );
        assertEquals( 4, seenRecord.getFirstPrevRel() );
        assertEquals( 42, currentRecord.getId() );
        assertEquals( 9, currentRecord.getFirstPrevRel() );
    }

    @Test
    void shouldReportRelationshipInconsistenciesFromDifferentLogs() throws IOException
    {
        // Given
        Path log1 = logFile( 1 );
        Path log2 = logFile( 2 );
        Path log3 = logFile( 3 );

        writeTxContent( log1, 0,
                new Command.RelationshipCommand(
                        createRelationshipRecord( 42, false, -1, -1, -1, -1, -1, -1, -1, false, false ),
                        createRelationshipRecord( 42, true, 1, 2, 3, 4, 5, 6, 7, true, true )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 5, false, -1, -1 ),
                        propertyRecord( 5, true, -1, -1, 777 )
                ),
                new Command.RelationshipCommand(
                        createRelationshipRecord( 21, true, 1, 2, 3, 4, 5, 6, 7, true, true ),
                        createRelationshipRecord( 21, false, -1, -1, -1, -1, -1, -1, -1, false, false )
                )
        );

        writeTxContent( log2, 0,
                new Command.RelationshipCommand(
                        createRelationshipRecord( 42, true, 1, 2, 3, 9, 5, 6, 7, true, true ),
                        createRelationshipRecord( 42, true, 1, 2, 3, 4, 5, 6, 7, true, true )
                )
        );

        writeTxContent( log3, 0,
                new Command.RelationshipCommand(
                        createRelationshipRecord( 53, true, 1, 2, 3, 4, 5, 6, 7, true, true ),
                        createRelationshipRecord( 53, true, 1, 2, 30, 4, 14, 6, 7, true, true )
                ),
                new Command.RelationshipCommand(
                        createRelationshipRecord( 42, true, 1, 2, 3, 4, 5, 6, 7, false, true ),
                        createRelationshipRecord( 42, true, 1, 2, 3, 4, 5, 6, 7, false, true )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( System.out, fs );

        // When
        checker.scan( getLogFiles(), handler, List.of( RELATIONSHIP ) );

        // Then
        assertEquals( 2, handler.recordInconsistencies.size() );

        RelationshipRecord seenRecord1 = (RelationshipRecord) handler.recordInconsistencies.get( 0 ).committed.record();
        RelationshipRecord currentRecord1 =
                (RelationshipRecord) handler.recordInconsistencies.get( 0 ).current.record();

        assertEquals( 42, seenRecord1.getId() );
        assertEquals( 4, seenRecord1.getFirstPrevRel() );
        assertEquals( 42, currentRecord1.getId() );
        assertEquals( 9, currentRecord1.getFirstPrevRel() );

        RelationshipRecord seenRecord2 = (RelationshipRecord) handler.recordInconsistencies.get( 1 ).committed.record();
        RelationshipRecord currentRecord2 =
                (RelationshipRecord) handler.recordInconsistencies.get( 1 ).current.record();

        assertEquals( 42, seenRecord2.getId() );
        assertTrue( seenRecord2.isFirstInFirstChain() );
        assertEquals( 42, currentRecord2.getId() );
        assertFalse( currentRecord2.isFirstInFirstChain() );
    }

    @Test
    void shouldReportRelationshipGroupInconsistenciesFromSingleLog() throws IOException
    {
        // Given
        Path log = logFile( 1 );

        writeTxContent( log, 0,
                new Command.RelationshipGroupCommand(
                        relationshipGroupRecord( 42, -1, -1, -1, -1, -1, -1, false ),
                        relationshipGroupRecord( 42, 1, 2, 3, 4, 5, 6, true )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 5, false, -1, -1 ),
                        propertyRecord( 5, true, -1, -1, 777 )
                ),
                new Command.RelationshipGroupCommand(
                        relationshipGroupRecord( 21, 1, 2, 3, 4, 5, 7, true ),
                        relationshipGroupRecord( 21, -1, -1, -1, -1, -1, -1, false )
                )
        );

        writeTxContent( log, 0,
                new Command.RelationshipGroupCommand(
                        relationshipGroupRecord( 53, 1, 2, 3, 4, 5, 6, true ),
                        relationshipGroupRecord( 53, 1, 2, 30, 4, 14, 6, true )
                ),
                new Command.RelationshipGroupCommand(
                        relationshipGroupRecord( 42, 1, 2, 3, 9, 5, 6, true ),
                        relationshipGroupRecord( 42, 1, 2, 3, 4, 5, 6, true )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( System.out, fs );

        // When
        checker.scan( getLogFiles(), handler, List.of( RELATIONSHIP_GROUP ) );

        // Then
        assertEquals( 1, handler.recordInconsistencies.size() );

        RelationshipGroupRecord seenRecord =
                (RelationshipGroupRecord) handler.recordInconsistencies.get( 0 ).committed.record();
        RelationshipGroupRecord currentRecord =
                (RelationshipGroupRecord) handler.recordInconsistencies.get( 0 ).current.record();

        assertEquals( 42, seenRecord.getId() );
        assertEquals( 4, seenRecord.getFirstLoop() );
        assertEquals( 42, currentRecord.getId() );
        assertEquals( 9, currentRecord.getFirstLoop() );
    }

    @Test
    void shouldReportRelationshipGroupInconsistenciesFromDifferentLogs() throws IOException
    {
        // Given
        Path log1 = logFile( 1 );
        Path log2 = logFile( 2 );
        Path log3 = logFile( 3 );

        writeTxContent( log1, 0,
                new Command.RelationshipGroupCommand(
                        relationshipGroupRecord( 42, -1, -1, -1, -1, -1, -1, false ),
                        relationshipGroupRecord( 42, 1, 2, 3, 4, 5, 6, true )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 5, false, -1, -1 ),
                        propertyRecord( 5, true, -1, -1, 777 )
                ),
                new Command.RelationshipGroupCommand(
                        relationshipGroupRecord( 21, 1, 2, 3, 4, 5, 6, true ),
                        relationshipGroupRecord( 21, -1, -1, -1, -1, -1, -1, false )
                )
        );

        writeTxContent( log2, 0,
                new Command.RelationshipGroupCommand(
                        relationshipGroupRecord( 42, 1, 2, 3, 9, 5, 6, true ),
                        relationshipGroupRecord( 42, 1, 2, 3, 4, 5, 6, true )
                )
        );

        writeTxContent( log3, 0,
                new Command.RelationshipGroupCommand(
                        relationshipGroupRecord( 53, 1, 2, 3, 4, 5, 6, true ),
                        relationshipGroupRecord( 53, 1, 2, 30, 4, 14, 6, true )
                ),
                new Command.RelationshipGroupCommand(
                        relationshipGroupRecord( 42, 1, 2, 3, 4, 5, 6, false ),
                        relationshipGroupRecord( 42, 1, 2, 3, 4, 5, 6, false )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( System.out, fs );

        // When
        checker.scan( getLogFiles(), handler, List.of( RELATIONSHIP_GROUP ) );

        // Then
        assertEquals( 2, handler.recordInconsistencies.size() );

        RelationshipGroupRecord seenRecord1 =
                (RelationshipGroupRecord) handler.recordInconsistencies.get( 0 ).committed.record();
        RelationshipGroupRecord currentRecord1 =
                (RelationshipGroupRecord) handler.recordInconsistencies.get( 0 ).current.record();

        assertEquals( 42, seenRecord1.getId() );
        assertEquals( 4, seenRecord1.getFirstLoop() );
        assertEquals( 42, currentRecord1.getId() );
        assertEquals( 9, currentRecord1.getFirstLoop() );

        RelationshipGroupRecord seenRecord2 =
                (RelationshipGroupRecord) handler.recordInconsistencies.get( 1 ).committed.record();
        RelationshipGroupRecord currentRecord2 =
                (RelationshipGroupRecord) handler.recordInconsistencies.get( 1 ).current.record();

        assertEquals( 42, seenRecord2.getId() );
        assertTrue( seenRecord2.inUse() );
        assertEquals( 42, currentRecord2.getId() );
        assertFalse( currentRecord2.inUse() );
    }

    @Test
    void shouldDetectAnInconsistentCheckPointPointingToALogFileGreaterThanMaxLogVersion() throws Exception
    {
        // given
        LogFiles logFiles = getLogFiles();
        LogPosition logPosition = new LogPosition( 2, CURRENT_FORMAT_LOG_HEADER_SIZE );
        try ( Lifespan lifespan = new Lifespan( logFiles ) )
        {
            var checkpointFile = logFiles.getCheckpointFile();
            var checkpointAppender = checkpointFile.getCheckpointAppender();
            checkpointAppender.checkPoint( LogCheckPointEvent.NULL, logPosition, Instant.now(), "test" );
        }

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( System.out, fs );

        // when
        checker.validateCheckPoints( getLogFiles(), handler );

        // then
        assertEquals( 1, handler.checkPointInconsistencies.size() );

        assertEquals( 0, handler.checkPointInconsistencies.get( 0 ).logVersion );
        assertEquals( logPosition, handler.checkPointInconsistencies.get( 0 ).logPosition );
        assertThat( handler.checkPointInconsistencies.get( 0 ).size ).isLessThan( 0L );
    }

    @Test
    void shouldDetectAnInconsistentCheckPointPointingToAByteOffsetNotInTheFile() throws Exception
    {
        // given
        LogFiles logFiles = getLogFiles();
        LogPosition logPosition = new LogPosition( 0, CURRENT_FORMAT_LOG_HEADER_SIZE + 42 );
        try ( Lifespan lifespan = new Lifespan( logFiles ) )
        {
            var checkpointFile = logFiles.getCheckpointFile();
            var checkpointAppender = checkpointFile.getCheckpointAppender();
            checkpointAppender.checkPoint( LogCheckPointEvent.NULL, logPosition, Instant.now(), "test" );
        }

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( System.out, fs );

        // when
        checker.validateCheckPoints( getLogFiles(), handler );

        // then
        assertEquals( 1, handler.checkPointInconsistencies.size() );

        assertEquals( 0, handler.checkPointInconsistencies.get( 0 ).logVersion );
        assertEquals( logPosition, handler.checkPointInconsistencies.get( 0 ).logPosition );
        assertEquals( CURRENT_FORMAT_LOG_HEADER_SIZE + 22, handler.checkPointInconsistencies.get( 0 ).size );
    }

    @Test
    void shouldNotReportInconsistencyIfTheCheckPointAreValidOrTheyReferToPrunedLogs() throws Exception
    {
        // given
        writeCheckPoint( logFile( 1 ), 0, CURRENT_FORMAT_LOG_HEADER_SIZE );
        writeCheckPoint( logFile( 2 ), 1, CURRENT_FORMAT_LOG_HEADER_SIZE );
        writeCheckPoint( logFile( 3 ), 3, CURRENT_FORMAT_LOG_HEADER_SIZE );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( System.out, fs );

        // when
        checker.validateCheckPoints( getLogFiles(), handler );

        // then
        assertTrue( handler.checkPointInconsistencies.isEmpty() );
    }

    @Test
    void shouldReportAnInconsistencyIfTxIdSequenceIsNotStrictlyIncreasing() throws Exception
    {
        // given
        LongFunction<Command.NodeCommand> newNodeCommandFunction =
                i -> new Command.NodeCommand( nodeRecord( i, false, false, -1, -1, -1 ),
                        nodeRecord( i, true, false, -1, -1, -1 ) );
        writeTxContent( logFile( 1 ), 40L, newNodeCommandFunction.apply( 1L ) );
        writeTxContent( logFile( 1 ), 41L, newNodeCommandFunction.apply( 2L ) );
        writeTxContent( logFile( 1 ), 42L, newNodeCommandFunction.apply( 3L ) );
        writeTxContent( logFile( 2 ), 42L, newNodeCommandFunction.apply( 4L ) );
        writeTxContent( logFile( 2 ), 43L, newNodeCommandFunction.apply( 5L ) );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( System.out, fs );

        // when
        checker.scan( getLogFiles(), handler, CHECK_TYPES );

        // then
        assertEquals( 1, handler.txIdSequenceInconsistencies.size() );
        assertEquals( 42, handler.txIdSequenceInconsistencies.get( 0 ).lastSeenTxId );
        assertEquals( 42, handler.txIdSequenceInconsistencies.get( 0 ).currentTxId );
    }

    @Test
    void shouldReportAnInconsistencyIfTxIdSequenceHasGaps() throws Exception
    {
        // given
        LongFunction<Command.NodeCommand> newNodeCommandFunction =
                i -> new Command.NodeCommand( nodeRecord( i, false, false, -1, -1, -1 ),
                        nodeRecord( i, true, false, -1, -1, -1 ) );
        writeTxContent( logFile( 1 ), 40L, newNodeCommandFunction.apply( 1L ) );
        writeTxContent( logFile( 1 ), 41L, newNodeCommandFunction.apply( 2L ) );
        writeTxContent( logFile( 1 ), 42L, newNodeCommandFunction.apply( 3L ) );
        writeTxContent( logFile( 2 ), 44L, newNodeCommandFunction.apply( 4L ) );
        writeTxContent( logFile( 2 ), 45L, newNodeCommandFunction.apply( 5L ) );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( System.out, fs );

        // when
        checker.scan( getLogFiles(), handler, CHECK_TYPES );

        // then
        assertEquals( 1, handler.txIdSequenceInconsistencies.size() );
        assertEquals( 42, handler.txIdSequenceInconsistencies.get( 0 ).lastSeenTxId );
        assertEquals( 44, handler.txIdSequenceInconsistencies.get( 0 ).currentTxId );
    }

    @Test
    void shouldReportNoInconsistenciesIfTxIdSequenceIsStrictlyIncreasingAndHasNoGaps() throws Exception
    {
        // given

        LongFunction<Command.NodeCommand> newNodeCommandFunction =
                i -> new Command.NodeCommand( nodeRecord( i, false, false, -1, -1, -1 ),
                        nodeRecord( i, true, false, -1, -1, -1 ) );
        writeTxContent( logFile( 1 ), 40L, newNodeCommandFunction.apply( 1L ) );
        writeTxContent( logFile( 1 ), 41L, newNodeCommandFunction.apply( 2L ) );
        writeTxContent( logFile( 1 ), 42L, newNodeCommandFunction.apply( 3L ) );
        writeTxContent( logFile( 2 ), 43L, newNodeCommandFunction.apply( 4L ) );
        writeTxContent( logFile( 2 ), 44L, newNodeCommandFunction.apply( 5L ) );
        writeTxContent( logFile( 2 ), 45L, newNodeCommandFunction.apply( 6L ) );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( System.out, fs );

        // when
        checker.scan( getLogFiles(), handler, CHECK_TYPES );

        // then
        assertTrue( handler.txIdSequenceInconsistencies.isEmpty() );
    }

    private static RelationshipRecord createRelationshipRecord( long id, boolean inUse, long firstNode, long secondNode, int type, long firstPrevRel,
            long firstNextRel, long secondPrevRel, long secondNextRel, boolean firstInFirstChain, boolean firstInSecondChain )
    {
        RelationshipRecord record = new RelationshipRecord( id );
        record.initialize( inUse, 0, firstNode, secondNode, type, firstPrevRel, firstNextRel, secondPrevRel, secondNextRel, firstInFirstChain,
                firstInSecondChain );
        return record;
    }

    private Path logFile( long version )
    {
        return databaseLayout.databaseDirectory().resolve( TransactionLogFilesHelper.DEFAULT_NAME + "." + version );
    }

    private static NodeRecord nodeRecord( long id, boolean inUse, boolean dense, long nextRel, long nextProp, long labels )
    {
        return new NodeRecord( id ).initialize( inUse, nextProp, dense, nextRel, labels );
    }

    private static PropertyRecord propertyRecord( long id, boolean inUse, long prevProp, long nextProp, long... blocks )
    {
        PropertyRecord record = new PropertyRecord( id );
        record.setInUse( inUse );
        record.setPrevProp( prevProp );
        record.setNextProp( nextProp );
        for ( int i = 0; i < blocks.length; i++ )
        {
            long blockValue = blocks[i];
            PropertyBlock block = new PropertyBlock();
            long value = PropertyStore.singleBlockLongValue( i, PropertyType.INT, blockValue );
            block.setSingleBlock( value );
            record.addPropertyBlock( block );
        }
        return record;
    }

    private static RelationshipGroupRecord relationshipGroupRecord( long id, int type, long firstOut, long firstIn, long firstLoop, long owningNode, long next,
            boolean inUse )
    {
        return new RelationshipGroupRecord( id ).initialize( inUse, type, firstOut, firstIn, firstLoop, owningNode, next );
    }

    private void writeTxContent( Path log, long txId, Command... commands ) throws IOException
    {
        PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( Arrays.asList( commands ) );
        tx.setHeader( new byte[0], 0, 0, 0, 0, AUTH_DISABLED );
        writeContent( log, txWriter -> txWriter.append( tx, txId, BASE_TX_CHECKSUM ) );
    }

    private void writeCheckPoint( Path log, long logVersion, long byteOffset ) throws IOException
    {
        LogPosition logPosition = new LogPosition( logVersion, byteOffset );
        writeContent( log, txWriter -> txWriter.legacyCheckPoint( logPosition ) );
    }

    private void writeContent( Path log, ThrowingConsumer<TransactionLogWriter,IOException> consumer )
            throws IOException
    {
        ensureLogExists( log );
        try ( StoreChannel channel = fs.write( log );
              var versionedChannel = new PhysicalLogVersionedStoreChannel( channel, 0, (byte) 0, log, EMPTY_ACCESSOR );
              var writableLogChannel = new PositionAwarePhysicalFlushableChecksumChannel( versionedChannel, new HeapScopedBuffer( 100, INSTANCE ) ) )
        {
            long offset = channel.size();
            channel.position( offset );

            consumer.accept( new TransactionLogWriter( writableLogChannel, new DbmsLogEntryWriterFactory( LogEntryV4_0::getVersionByte ) ) );
        }
    }

    private void ensureLogExists( Path logFile ) throws IOException
    {
        if ( !fs.fileExists( logFile ) )
        {
            try ( StoreChannel channel = fs.write( logFile ) )
            {
                writeLogHeader( channel, new LogHeader( getLogFiles().getLogFile().getLogVersion( logFile ), 0, StoreId.UNKNOWN ), INSTANCE );
            }
        }
    }

    private static class CapturingInconsistenciesHandler implements InconsistenciesHandler
    {
        List<TxIdSequenceInconsistency> txIdSequenceInconsistencies = new ArrayList<>();
        List<CheckPointInconsistency> checkPointInconsistencies = new ArrayList<>();
        List<RecordInconsistency> recordInconsistencies = new ArrayList<>();

        @Override
        public void reportInconsistentCheckPoint( long logVersion, LogPosition logPosition, long size )
        {
            checkPointInconsistencies.add( new CheckPointInconsistency( logVersion, logPosition, size ) );
        }

        @Override
        public void reportInconsistentCommand( RecordInfo<?> committed, RecordInfo<?> current )
        {
            recordInconsistencies.add( new RecordInconsistency( committed, current ) );
        }

        @Override
        public void reportInconsistentTxIdSequence( long lastSeenTxId, long currentTxId )
        {
            txIdSequenceInconsistencies.add( new TxIdSequenceInconsistency( lastSeenTxId, currentTxId ) );
        }
    }

    private static class TxIdSequenceInconsistency
    {
        final long lastSeenTxId;
        final long currentTxId;

        private TxIdSequenceInconsistency( long lastSeenTxId, long currentTxId )
        {
            this.lastSeenTxId = lastSeenTxId;
            this.currentTxId = currentTxId;
        }

        @Override
        public String toString()
        {
            return "TxIdSequenceInconsistency{" + "lastSeenTxId=" + lastSeenTxId + ", currentTxId=" + currentTxId + '}';
        }
    }

    private static class CheckPointInconsistency
    {
        final long logVersion;
        final LogPosition logPosition;
        final long size;

        CheckPointInconsistency( long logVersion, LogPosition logPosition, Long size )
        {
            this.logVersion = logVersion;
            this.logPosition = logPosition;
            this.size = size;
        }
    }

    private static class RecordInconsistency
    {
        final RecordInfo<?> committed;
        final RecordInfo<?> current;

        RecordInconsistency( RecordInfo<?> committed, RecordInfo<?> current )
        {
            this.committed = committed;
            this.current = current;
        }
    }
}
