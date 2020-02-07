package org.neo4j.internal.recordstorage;

import org.neo4j.storageengine.api.StorageCommonCommand;

import java.io.IOException;


/**
 * An interface for dealing with commands, either reading or writing them. See also {@link TransactionApplier}. The
 * methods in this class should almost always return false, unless something went wrong.
 */
public interface CommonCommandVisitor {
    // Store commands
    boolean visitNodeCountsCommand(StorageCommonCommand.NodeCountsCommand command) throws IOException;

    boolean visitRelationshipCountsCommand(StorageCommonCommand.RelationshipCountsCommand command) throws IOException;

}
