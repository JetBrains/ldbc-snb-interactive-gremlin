# Adding a New Database Vendor

Requirements:
1. Create a vendor module
2. Implement `GraphProvider`
3. Create a Guice module
4. Register vendor in runner
5. Optionally override default queries

## Step 1: Create Vendor Module

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

<dependencies>
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
</dependencies>
```

## Step 2: Implement GraphProvider

Reference: `common/src/main/java/com/youtrackdb/ldbc/common/GraphProvider.java`

```java
public class YourDbGraphProvider implements GraphProvider {
    @Inject
    public YourDbGraphProvider() {
    }

    @Override
    public <E extends Exception> void executeInTx(FailableConsumer<GraphTraversalSource, E> code) throws E {
    }

    @Override
    public <E extends Exception, R> R computeInTx(FailableFunction<GraphTraversalSource, R, E> code) throws E {
    }

    @Override
    public void close() throws IOException {
    }
}
```

## Step 3: Create Guice Module

Reference: `ytdb/src/main/java/com/youtrackdb/ldbc/ytdb/YtdbModule.java`

```java
public class YourDbModule extends AbstractModule {
    public YourDbModule(Map<String, String> properties) {
    }

    @Override
    protected void configure() {
        bind(GraphProvider.class).to(YourDbGraphProvider.class);
    }
}
```

## Step 4: Register Vendor

Add to `TinkerPopDb.createVendorModule()`:

Reference: `runner/src/main/java/com/youtrackdb/ldbc/runner/TinkerPopDb.java`

```java
return switch (vendor.toLowerCase()) {
    case "ytdb" -> new YtdbModule(properties);
    case "yourdb" -> new YourDbModule(properties);
    default -> throw new DbException("Unknown vendor: " + vendor);
};
```

## Step 5: Configure Properties

Set in `runner/ldbc-driver.properties`:

```properties
tinkerpop.vendor=yourdb
yourdb.host=localhost
yourdb.port=8182
```

## Overriding Queries (Optional)

Override default queries for vendor-specific optimizations.

Reference: `ytdb/src/main/java/com/youtrackdb/ldbc/ytdb/OptimizedShortQuery1.java`

```java
public class OptimizedShortQuery1 implements OperationHandler<LdbcShortQuery1PersonProfile, TinkerPopConnectionState> {
    @Override
    public void executeOperation(
            LdbcShortQuery1PersonProfile operation,
            TinkerPopConnectionState state,
            ResultReporter resultReporter) throws DbException {
    }
}
```

Bind in module:

```java
OperationBindings.bindQuery(
    this.binder(),
    LdbcShortQuery1PersonProfile.class,
    OptimizedShortQuery1.class
);
```

Default implementations: `common/src/main/java/com/youtrackdb/ldbc/common/DefaultQueryModule.java`

## Data Loading

Implement vendor-specific CSV loading. Follow schema in `common/src/main/java/com/youtrackdb/ldbc/common/LdbcSchema.java`.

Reference: `ytdb/src/main/java/com/youtrackdb/ldbc/ytdb/loader/`

## Testing

```bash
# Load test data into your database
./scripts/ldbc-driver.sh
# Check runner/results/
```
