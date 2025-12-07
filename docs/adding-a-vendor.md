# Adding a New Database Vendor

## Overview

Adding a TinkerPop-compatible database requires:
1. Creating a vendor module
2. Implementing `GraphProvider`
3. Creating a Guice module
4. Registering the vendor in the runner
5. Optionally overriding default queries

## Step 1: Create Vendor Module

Create a new Maven module for your database:

```
your-db/
    pom.xml
    src/main/java/com/youtrackdb/ldbc/yourdb/
        YourDbModule.java
        YourDbGraphProvider.java
```

### pom.xml

```xml
<parent>
    <groupId>com.youtrackdb.ldbc</groupId>
    <artifactId>ldbc-snb-tinkerpop</artifactId>
    <version>1.0-SNAPSHOT</version>
</parent>

<artifactId>yourdb</artifactId>
<name>YourDB Implementation</name>

<dependencies>
    <!-- Core dependencies -->
    <dependency>
        <groupId>com.youtrackdb.ldbc</groupId>
        <artifactId>common</artifactId>
    </dependency>
    <dependency>
        <groupId>org.ldbcouncil.snb</groupId>
        <artifactId>driver</artifactId>
    </dependency>
    <dependency>
        <groupId>org.apache.tinkerpop</groupId>
        <artifactId>gremlin-core</artifactId>
    </dependency>
    <dependency>
        <groupId>com.google.inject</groupId>
        <artifactId>guice</artifactId>
    </dependency>

    <!-- Your database driver -->
    <dependency>
        <groupId>your.db</groupId>
        <artifactId>yourdb-driver</artifactId>
        <version>x.y.z</version>
    </dependency>
</dependencies>
```

## Step 2: Implement GraphProvider

The `GraphProvider` interface requires two methods for transaction management.

Reference: `common/src/main/java/com/youtrackdb/ldbc/common/GraphProvider.java:9`

```java
package com.youtrackdb.ldbc.yourdb;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.youtrackdb.ldbc.common.GraphProvider;
import org.apache.commons.lang3.function.FailableConsumer;
import org.apache.commons.lang3.function.FailableFunction;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.io.IOException;
import java.util.Map;

public class YourDbGraphProvider implements GraphProvider {

    private final YourDbConnection connection;
    private final GraphTraversalSource traversal;

    @Inject
    public YourDbGraphProvider(@Named("properties") Map<String, String> properties) {
        // Read vendor-specific properties
        String host = properties.get("yourdb.host");
        String port = properties.get("yourdb.port");
        // ... other config

        // Initialize connection and traversal source
        this.connection = YourDbConnection.create(host, port);
        this.traversal = connection.traversal();
    }

    @Override
    public <E extends Exception> void executeInTx(FailableConsumer<GraphTraversalSource, E> code) throws E {
        // Execute code within transaction
        // Auto-commit on success, rollback on exception
        try {
            code.accept(traversal);
            traversal.tx().commit();
        } catch (Exception e) {
            traversal.tx().rollback();
            throw e;
        }
    }

    @Override
    public <E extends Exception, R> R computeInTx(FailableFunction<GraphTraversalSource, R, E> code) throws E {
        // Execute code within transaction and return result
        try {
            R result = code.apply(traversal);
            traversal.tx().commit();
            return result;
        } catch (Exception e) {
            traversal.tx().rollback();
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        if (traversal != null) {
            traversal.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
```

## Step 3: Create Guice Module

Wire your `GraphProvider` implementation into dependency injection.

Reference: `ytdb/src/main/java/com/youtrackdb/ldbc/ytdb/YtdbModule.java:14`

```java
package com.youtrackdb.ldbc.yourdb;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.youtrackdb.ldbc.common.GraphProvider;

import java.util.Map;

public class YourDbModule extends AbstractModule {

    private final Map<String, String> properties;

    public YourDbModule(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    protected void configure() {
        // Make properties available for injection
        bind(new TypeLiteral<Map<String, String>>() {})
            .annotatedWith(Names.named("properties"))
            .toInstance(properties);

        // Bind GraphProvider to your implementation
        bind(GraphProvider.class).to(YourDbGraphProvider.class);
    }
}
```

## Step 4: Register Vendor in Runner

Add your vendor to the switch statement in `TinkerPopDb.createVendorModule()`.

Reference: `runner/src/main/java/com/youtrackdb/ldbc/runner/TinkerPopDb.java:51`

```java
private Module createVendorModule(String vendor, Map<String, String> properties) throws DbException {
    return switch (vendor.toLowerCase()) {
        case "ytdb" -> new YtdbModule(properties);
        case "yourdb" -> new YourDbModule(properties);  // Add this line
        default -> throw new DbException("Unknown vendor: " + vendor + ". Supported: ytdb, yourdb");
    };
}
```

## Step 5: Configure Properties

Set `tinkerpop.vendor` in `runner/ldbc-driver.properties`:

```properties
tinkerpop.vendor=yourdb

# Your database-specific properties
yourdb.host=localhost
yourdb.port=8182
yourdb.database=snb
yourdb.username=admin
yourdb.password=secret
```

## Overriding Queries (Optional)

Override default queries when your database supports vendor-specific optimizations.

### Implement OperationHandler

Reference: `ytdb/src/main/java/com/youtrackdb/ldbc/ytdb/OptimizedShortQuery1.java:21`

```java
package com.youtrackdb.ldbc.yourdb;

import com.youtrackdb.ldbc.common.TinkerPopConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery1PersonProfile;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery1PersonProfileResult;

public class OptimizedShortQuery1 implements OperationHandler<LdbcShortQuery1PersonProfile, TinkerPopConnectionState> {

    @Override
    public void executeOperation(
            LdbcShortQuery1PersonProfile operation,
            TinkerPopConnectionState state,
            ResultReporter resultReporter) throws DbException {

        // Your optimized implementation
        try {
            // Build and execute query
            // Report results
        } catch (Exception e) {
            throw new DbException("Query execution failed", e);
        }
    }
}
```

### Bind Override in Module

Reference: `ytdb/src/main/java/com/youtrackdb/ldbc/ytdb/YtdbModule.java`

```java
import com.youtrackdb.ldbc.common.OperationBindings;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery1PersonProfile;

public class YourDbModule extends AbstractModule {
    @Override
    protected void configure() {
        // ... existing bindings ...

        // Override specific queries
        OperationBindings.bindQuery(
            this.binder(),
            LdbcShortQuery1PersonProfile.class,
            OptimizedShortQuery1.class
        );
    }
}
```

Default implementations are in `common/src/main/java/com/youtrackdb/ldbc/common/DefaultQueryModule.java` and only need to be overridden if vendor-specific optimization is required.

## Data Loading

Implement vendor-specific data loading from CSV files. LDBC test data must be loaded before running validation or benchmarks.

Key considerations:
- Load data from `test-data/` (if you used provided scripts to download the data, it should be in `test-data/runtime/`)
- Follow the LDBC SNB schema (see `common/src/main/java/com/youtrackdb/ldbc/common/LdbcSchema.java`)
- Data must be reloaded after each benchmark run (updates modify the dataset)

## Testing

1. Load test data into your database
2. Run validation: `./scripts/ldbc-driver.sh`
3. Check `runner/results/` for validation results

The default configuration runs validation mode with scale factor 0.1 and 200 validation parameters.
