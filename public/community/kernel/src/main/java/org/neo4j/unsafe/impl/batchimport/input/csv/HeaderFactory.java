package org.neo4j.unsafe.impl.batchimport.input.csv;

import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.unsafe.impl.batchimport.input.Groups;

public interface HeaderFactory
{
    /**
     * @param dataSeeker {@link CharSeeker} containing the data. Usually there's a header for us
     * to read at the very top of it.
     * @param configuration {@link Configuration} specific to the format of the data.
     * @param idType type of values we expect the ids to be.
     * @param groups {@link Groups} to register groups in.
     * @return the created {@link Header}.
     */
    Header create(CharSeeker dataSeeker, Configuration configuration, IdType idType, Groups groups);

    /**
     * @return whether or not this header is already defined. If this returns {@code false} then the header
     * will be read from the top of the data stream.
     */
    boolean isDefined();
}
