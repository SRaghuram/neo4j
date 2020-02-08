package org.neo4j.storageengine;

import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionCheck;

import java.util.Optional;

public class MyStoreVersionCheck implements StoreVersionCheck {


    @Override
    public Optional<String> storeVersion() {
        return Optional.empty();
    }

    @Override
    public String configuredVersion() {
        return MyStoreVersion.MyStandardFormat;
    }

    @Override
    public StoreVersion versionInformation(String storeVersion) {
        if (storeVersion.equalsIgnoreCase(MyStoreVersion.MyStandardFormat))
            return new MyStoreVersion();
        throw new IllegalArgumentException( "Unknown store version '" + storeVersion + "'" );
    }

    @Override
    public Result checkUpgrade(String desiredVersion) {
        return new Result( Outcome.ok, MyStoreVersion.MyStandardFormat, null );
    }
}
