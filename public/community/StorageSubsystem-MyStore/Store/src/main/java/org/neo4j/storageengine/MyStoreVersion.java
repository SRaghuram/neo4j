package org.neo4j.storageengine;

import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.format.Capability;
import org.neo4j.storageengine.api.format.CapabilityType;

import java.util.Optional;

public class MyStoreVersion implements StoreVersion {

    public static final String MyStandardFormat = "My0.0.0";

    @Override
    public String storeVersion() {
        return null;
    }

    @Override
    public boolean hasCapability(Capability capability) {
        return false;
    }

    @Override
    public boolean hasCompatibleCapabilities(StoreVersion otherVersion, CapabilityType type) {
        return false;
    }

    @Override
    public String introductionNeo4jVersion() {
        return null;
    }

    @Override
    public Optional<StoreVersion> successor() {
        return Optional.empty();
    }

    @Override
    public boolean isCompatibleWith(StoreVersion otherVersion) {
        return false;
    }
}
