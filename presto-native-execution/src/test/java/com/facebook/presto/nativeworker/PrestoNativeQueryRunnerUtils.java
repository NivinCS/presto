/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.nativeworker;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.functionNamespace.FunctionNamespaceManagerPlugin;
import com.facebook.presto.functionNamespace.json.JsonFileBasedFunctionNamespaceManagerFactory;
import com.facebook.presto.hive.HiveQueryRunner;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.iceberg.FileFormat;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.io.Resources;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.function.BiFunction;

import static com.facebook.presto.common.ErrorType.INTERNAL_ERROR;
import static com.facebook.presto.hive.HiveQueryRunner.METASTORE_CONTEXT;
import static com.facebook.presto.hive.HiveQueryRunner.createDatabaseMetastoreObject;
import static com.facebook.presto.hive.HiveQueryRunner.getFileHiveMetastore;
import static com.facebook.presto.hive.HiveTestUtils.getProperty;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.getNativeSidecarProperties;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.getNativeWorkerHiveProperties;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.getNativeWorkerIcebergProperties;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.getNativeWorkerSystemProperties;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.getNativeWorkerTpcdsProperties;
import static com.facebook.presto.nativeworker.SymlinkManifestGeneratorUtils.createSymlinkManifest;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertTrue;

public class PrestoNativeQueryRunnerUtils
{
    // The unix domain socket (UDS) used to communicate with the remote function server.
    public static final String REMOTE_FUNCTION_UDS = "remote_function_server.socket";
    public static final String REMOTE_FUNCTION_JSON_SIGNATURES = "remote_function_server.json";
    public static final String REMOTE_FUNCTION_CATALOG_NAME = "remote";
    public static final String HIVE_DATA = "hive_data";

    protected static final String ICEBERG_DEFAULT_STORAGE_FORMAT = "PARQUET";

    private static final Logger log = Logger.get(PrestoNativeQueryRunnerUtils.class);
    private static final String DEFAULT_STORAGE_FORMAT = "DWRF";
    private static final String SYMLINK_FOLDER = "symlink_tables_manifests";
    private static final PrincipalPrivileges PRINCIPAL_PRIVILEGES = new PrincipalPrivileges(ImmutableMultimap.of(), ImmutableMultimap.of());
    private static final ErrorCode CREATE_ERROR_CODE = new ErrorCode(123, "CREATE_ERROR_CODE", INTERNAL_ERROR);

    private static final StorageFormat STORAGE_FORMAT_SYMLINK_TABLE = StorageFormat.create(
            ParquetHiveSerDe.class.getName(),
            SymlinkTextInputFormat.class.getName(),
            HiveIgnoreKeyTextOutputFormat.class.getName());

    private PrestoNativeQueryRunnerUtils() {}

    public static QueryRunner createQueryRunner(boolean addStorageFormatToPath, boolean isCoordinatorSidecarEnabled, boolean enableRuntimeMetricsCollection, boolean enableSsdCache)
            throws Exception
    {
        int cacheMaxSize = 4096; // 4GB size cache
        NativeQueryRunnerParameters nativeQueryRunnerParameters = getNativeQueryRunnerParameters();
        return createQueryRunner(
                Optional.of(nativeQueryRunnerParameters.serverBinary.toString()),
                Optional.of(nativeQueryRunnerParameters.dataDirectory),
                nativeQueryRunnerParameters.workerCount,
                cacheMaxSize,
                DEFAULT_STORAGE_FORMAT,
                addStorageFormatToPath,
                isCoordinatorSidecarEnabled,
                enableRuntimeMetricsCollection,
                enableSsdCache);
    }

    public static QueryRunner createQueryRunner(
            Optional<String> prestoServerPath,
            Optional<Path> dataDirectory,
            Optional<Integer> workerCount,
            int cacheMaxSize,
            String storageFormat,
            boolean addStorageFormatToPath,
            boolean isCoordinatorSidecarEnabled,
            boolean enableRuntimeMetricsCollection,
            boolean enableSsdCache)
            throws Exception
    {
        QueryRunner defaultQueryRunner = createJavaQueryRunner(dataDirectory, storageFormat, addStorageFormatToPath);

        if (!prestoServerPath.isPresent()) {
            return defaultQueryRunner;
        }

        defaultQueryRunner.close();

        return createNativeQueryRunner(dataDirectory.get().toString(), prestoServerPath.get(), workerCount, cacheMaxSize, true, Optional.empty(),
                storageFormat, addStorageFormatToPath, false, isCoordinatorSidecarEnabled, false, enableRuntimeMetricsCollection, enableSsdCache, Collections.emptyMap());
    }

    public static QueryRunner createJavaQueryRunner()
            throws Exception
    {
        return createJavaQueryRunner(true);
    }

    public static QueryRunner createJavaQueryRunner(boolean addStorageFormatToPath)
            throws Exception
    {
        return createJavaQueryRunner(DEFAULT_STORAGE_FORMAT, addStorageFormatToPath);
    }

    public static QueryRunner createJavaQueryRunner(String storageFormat)
            throws Exception
    {
        return createJavaQueryRunner(Optional.of(getNativeQueryRunnerParameters().dataDirectory), storageFormat, true);
    }

    public static QueryRunner createJavaQueryRunner(String storageFormat, boolean addStorageFormatToPath)
            throws Exception
    {
        return createJavaQueryRunner(Optional.of(getNativeQueryRunnerParameters().dataDirectory), storageFormat, addStorageFormatToPath);
    }

    public static QueryRunner createJavaQueryRunner(Optional<Path> dataDirectory, String storageFormat, boolean addStorageFormatToPath)
            throws Exception
    {
        return createJavaQueryRunner(dataDirectory, "sql-standard", storageFormat, addStorageFormatToPath);
    }

    public static QueryRunner createJavaQueryRunner(Optional<Path> baseDataDirectory, String security, String storageFormat, boolean addStorageFormatToPath)
            throws Exception
    {
        ImmutableMap.Builder<String, String> hivePropertiesBuilder = new ImmutableMap.Builder<>();
        hivePropertiesBuilder
                .put("hive.storage-format", storageFormat)
                .put("hive.pushdown-filter-enabled", "true");

        if ("legacy".equals(security)) {
            hivePropertiesBuilder.put("hive.allow-drop-table", "true");
        }

        Optional<Path> dataDirectory = addStorageFormatToPath ? baseDataDirectory.map(path -> Paths.get(path.toString() + '/' + storageFormat)) : baseDataDirectory;
        DistributedQueryRunner queryRunner =
                HiveQueryRunner.createQueryRunner(
                        ImmutableList.of(),
                        ImmutableMap.of(
                                "regex-library", "RE2J",
                                "offset-clause-enabled", "true"),
                        security,
                        hivePropertiesBuilder.build(),
                        dataDirectory,
                        getNativeWorkerTpcdsProperties());
        return queryRunner;
    }

    public static void createSchemaIfNotExist(QueryRunner queryRunner, String schemaName)
    {
        ExtendedHiveMetastore metastore = getFileHiveMetastore((DistributedQueryRunner) queryRunner);
        if (!metastore.getDatabase(METASTORE_CONTEXT, schemaName).isPresent()) {
            metastore.createDatabase(METASTORE_CONTEXT, createDatabaseMetastoreObject(schemaName));
        }
    }

    public static void createExternalTable(QueryRunner queryRunner, String sourceSchemaName, String tableName, List<Column> columns, String targetSchemaName)
    {
        ExtendedHiveMetastore metastore = getFileHiveMetastore((DistributedQueryRunner) queryRunner);
        File dataDirectory = ((DistributedQueryRunner) queryRunner).getCoordinator().getDataDirectory().resolve(HIVE_DATA).toFile();
        Path hiveTableDataPath = dataDirectory.toPath().resolve(sourceSchemaName).resolve(tableName);
        Path symlinkTableDataPath = dataDirectory.toPath().getParent().resolve(SYMLINK_FOLDER).resolve(tableName);

        try {
            createSymlinkManifest(hiveTableDataPath, symlinkTableDataPath);
        }
        catch (IOException e) {
            throw new PrestoException(() -> CREATE_ERROR_CODE, "Failed to create symlink manifest file for table: " + tableName, e);
        }

        createSchemaIfNotExist(queryRunner, targetSchemaName);
        if (!metastore.getTable(METASTORE_CONTEXT, targetSchemaName, tableName).isPresent()) {
            metastore.createTable(METASTORE_CONTEXT, createHiveSymlinkTable(targetSchemaName, tableName, columns, symlinkTableDataPath.toString()), PRINCIPAL_PRIVILEGES, emptyList());
        }
    }

    public static QueryRunner createJavaIcebergQueryRunner(boolean addStorageFormatToPath)
            throws Exception
    {
        return createJavaIcebergQueryRunner(Optional.of(getNativeQueryRunnerParameters().dataDirectory), ICEBERG_DEFAULT_STORAGE_FORMAT, addStorageFormatToPath);
    }

    public static QueryRunner createJavaIcebergQueryRunner(String storageFormat)
            throws Exception
    {
        return createJavaIcebergQueryRunner(Optional.of(getNativeQueryRunnerParameters().dataDirectory), storageFormat, false);
    }

    public static QueryRunner createJavaIcebergQueryRunner(Optional<Path> baseDataDirectory, String storageFormat, boolean addStorageFormatToPath)
            throws Exception
    {
        ImmutableMap.Builder<String, String> icebergPropertiesBuilder = new ImmutableMap.Builder<>();
        icebergPropertiesBuilder.put("hive.parquet.writer.version", "PARQUET_1_0");

        return IcebergQueryRunner.builder()
                .setExtraProperties(ImmutableMap.of(
                        "regex-library", "RE2J",
                        "offset-clause-enabled", "true",
                        "query.max-stage-count", "110"))
                .setExtraConnectorProperties(icebergPropertiesBuilder.build())
                .setAddJmxPlugin(false)
                .setCreateTpchTables(false)
                .setDataDirectory(baseDataDirectory)
                .setAddStorageFormatToPath(addStorageFormatToPath)
                .setFormat(FileFormat.valueOf(storageFormat))
                .setTpcdsProperties(getNativeWorkerTpcdsProperties())
                .build().getQueryRunner();
    }

    public static QueryRunner createNativeIcebergQueryRunner(boolean useThrift)
            throws Exception
    {
        return createNativeIcebergQueryRunner(useThrift, ICEBERG_DEFAULT_STORAGE_FORMAT, Optional.empty());
    }

    public static QueryRunner createNativeIcebergQueryRunner(boolean useThrift, boolean addStorageFormatToPath)
            throws Exception
    {
        return createNativeIcebergQueryRunner(useThrift, ICEBERG_DEFAULT_STORAGE_FORMAT, Optional.empty(), addStorageFormatToPath);
    }

    public static QueryRunner createNativeIcebergQueryRunner(boolean useThrift, String storageFormat)
            throws Exception
    {
        return createNativeIcebergQueryRunner(useThrift, storageFormat, Optional.empty());
    }

    public static QueryRunner createNativeIcebergQueryRunner(boolean useThrift, String storageFormat, Optional<String> remoteFunctionServerUds)
            throws Exception
    {
        return createNativeIcebergQueryRunner(useThrift, storageFormat, remoteFunctionServerUds, false);
    }

    public static QueryRunner createNativeIcebergQueryRunner(boolean useThrift, String storageFormat, Optional<String> remoteFunctionServerUds, boolean addStorageFormatToPath)
            throws Exception
    {
        int cacheMaxSize = 0;
        NativeQueryRunnerParameters nativeQueryRunnerParameters = getNativeQueryRunnerParameters();
        return createNativeIcebergQueryRunner(
                Optional.of(nativeQueryRunnerParameters.dataDirectory),
                nativeQueryRunnerParameters.serverBinary.toString(),
                nativeQueryRunnerParameters.workerCount,
                cacheMaxSize,
                useThrift,
                remoteFunctionServerUds,
                storageFormat,
                addStorageFormatToPath);
    }

    public static QueryRunner createNativeIcebergQueryRunner(
            Optional<Path> dataDirectory,
            String prestoServerPath,
            Optional<Integer> workerCount,
            int cacheMaxSize,
            boolean useThrift,
            Optional<String> remoteFunctionServerUds,
            String storageFormat,
            boolean addStorageFormatToPath)
            throws Exception
    {
        ImmutableMap<String, String> icebergProperties = ImmutableMap.<String, String>builder()
                .putAll(getNativeWorkerIcebergProperties())
                .build();

        // Make query runner with external workers for tests
        return IcebergQueryRunner.builder()
                .setExtraProperties(ImmutableMap.<String, String>builder()
                        .put("http-server.http.port", "8080")
                        .put("experimental.internal-communication.thrift-transport-enabled", String.valueOf(useThrift))
                        .put("query.max-stage-count", "110")
                        .putAll(getNativeWorkerSystemProperties())
                        .build())
                .setFormat(FileFormat.valueOf(storageFormat))
                .setCreateTpchTables(false)
                .setAddJmxPlugin(false)
                .setNodeCount(OptionalInt.of(workerCount.orElse(4)))
                .setExternalWorkerLauncher(getExternalWorkerLauncher("iceberg", prestoServerPath, cacheMaxSize, remoteFunctionServerUds, false, false, false, false))
                .setAddStorageFormatToPath(addStorageFormatToPath)
                .setDataDirectory(dataDirectory)
                .setTpcdsProperties(getNativeWorkerTpcdsProperties())
                .build().getQueryRunner();
    }

    public static QueryRunner createNativeQueryRunner(
            String dataDirectory,
            String prestoServerPath,
            Optional<Integer> workerCount,
            int cacheMaxSize,
            boolean useThrift,
            Optional<String> remoteFunctionServerUds,
            String storageFormat,
            boolean addStorageFormatToPath,
            Boolean failOnNestedLoopJoin,
            boolean isCoordinatorSidecarEnabled,
            boolean singleNodeExecutionEnabled,
            boolean enableRuntimeMetricsCollection,
            boolean enableSsdCache,
            Map<String, String> extraProperties)
            throws Exception
    {
        // The property "hive.allow-drop-table" needs to be set to true because security is always "legacy" in NativeQueryRunner.
        ImmutableMap<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                .putAll(getNativeWorkerHiveProperties(storageFormat))
                .put("hive.allow-drop-table", "true")
                .build();

        ImmutableMap.Builder<String, String> coordinatorProperties = ImmutableMap.builder();
        coordinatorProperties.put("native-execution-enabled", "true");
        if (singleNodeExecutionEnabled) {
            coordinatorProperties.put("single-node-execution-enabled", "true");
        }

        // Make query runner with external workers for tests
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.<String, String>builder()
                        .put("http-server.http.port", "8081")
                        .put("experimental.internal-communication.thrift-transport-enabled", String.valueOf(useThrift))
                        .putAll(getNativeWorkerSystemProperties())
                        .putAll(isCoordinatorSidecarEnabled ? getNativeSidecarProperties() : ImmutableMap.of())
                        .putAll(extraProperties)
                        .build(),
                coordinatorProperties.build(),
                "legacy",
                hiveProperties,
                workerCount,
                Optional.of(Paths.get(addStorageFormatToPath ? dataDirectory + "/" + storageFormat : dataDirectory)),
                getExternalWorkerLauncher("hive", prestoServerPath, cacheMaxSize, remoteFunctionServerUds, failOnNestedLoopJoin,
                        isCoordinatorSidecarEnabled, enableRuntimeMetricsCollection, enableSsdCache),
                getNativeWorkerTpcdsProperties());
    }

    public static QueryRunner createNativeCteQueryRunner(boolean useThrift, String storageFormat)
            throws Exception
    {
        return createNativeCteQueryRunner(useThrift, storageFormat, true);
    }

    public static QueryRunner createNativeCteQueryRunner(boolean useThrift, String storageFormat, boolean addStorageFormatToPath)
            throws Exception
    {
        int cacheMaxSize = 0;

        NativeQueryRunnerParameters nativeQueryRunnerParameters = getNativeQueryRunnerParameters();
        String dataDirectory = nativeQueryRunnerParameters.dataDirectory.toString();
        String prestoServerPath = nativeQueryRunnerParameters.serverBinary.toString();
        Optional<Integer> workerCount = nativeQueryRunnerParameters.workerCount;

        // The property "hive.allow-drop-table" needs to be set to true because security is always "legacy" in NativeQueryRunner.
        ImmutableMap<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                .putAll(getNativeWorkerHiveProperties(storageFormat))
                .put("hive.allow-drop-table", "true")
                .put("hive.enable-parquet-dereference-pushdown", "true")
                .put("hive.temporary-table-compression-codec", "NONE")
                .put("hive.temporary-table-storage-format", storageFormat)
                .build();

        // Make query runner with external workers for tests
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.<String, String>builder()
                        .put("http-server.http.port", "8081")
                        .put("experimental.internal-communication.thrift-transport-enabled", String.valueOf(useThrift))
                        .putAll(getNativeWorkerSystemProperties())
                        .put("query.cte-partitioning-provider-catalog", "hive")
                        .build(),
                ImmutableMap.of(),
                "legacy",
                hiveProperties,
                workerCount,
                Optional.of(Paths.get(addStorageFormatToPath ? dataDirectory + "/" + storageFormat : dataDirectory)),
                getExternalWorkerLauncher("hive", prestoServerPath, cacheMaxSize, Optional.empty(), false, false, false, false),
                getNativeWorkerTpcdsProperties());
    }

    public static QueryRunner createNativeQueryRunner(String remoteFunctionServerUds)
            throws Exception
    {
        return createNativeQueryRunner(false, DEFAULT_STORAGE_FORMAT, Optional.ofNullable(remoteFunctionServerUds), false, false, false, false, false);
    }

    public static QueryRunner createNativeQueryRunner(Map<String, String> extraProperties, String storageFormat)
            throws Exception
    {
        int cacheMaxSize = 0;
        NativeQueryRunnerParameters nativeQueryRunnerParameters = getNativeQueryRunnerParameters();
        return createNativeQueryRunner(
                nativeQueryRunnerParameters.dataDirectory.toString(),
                nativeQueryRunnerParameters.serverBinary.toString(),
                nativeQueryRunnerParameters.workerCount,
                cacheMaxSize,
                true,
                Optional.empty(),
                storageFormat,
                true,
                false,
                false,
                false,
                false,
                false,
                extraProperties);
    }

    public static QueryRunner createNativeQueryRunner(boolean useThrift)
            throws Exception
    {
        return createNativeQueryRunner(useThrift, DEFAULT_STORAGE_FORMAT);
    }

    public static QueryRunner createNativeQueryRunner(boolean useThrift, boolean failOnNestedLoopJoin)
            throws Exception
    {
        return createNativeQueryRunner(useThrift, DEFAULT_STORAGE_FORMAT, Optional.empty(), failOnNestedLoopJoin, false, false, false, false);
    }

    public static QueryRunner createNativeQueryRunner(boolean useThrift, String storageFormat)
            throws Exception
    {
        return createNativeQueryRunner(useThrift, storageFormat, Optional.empty(), false, false, false, false, false);
    }

    public static QueryRunner createNativeQueryRunner(
            boolean useThrift,
            String storageFormat,
            Optional<String> remoteFunctionServerUds,
            Boolean failOnNestedLoopJoin,
            boolean isCoordinatorSidecarEnabled,
            boolean singleNodeExecutionEnabled,
            boolean enableRuntimeMetricsCollection,
            boolean enableSSDCache)
            throws Exception
    {
        int cacheMaxSize = 0;
        NativeQueryRunnerParameters nativeQueryRunnerParameters = getNativeQueryRunnerParameters();
        return createNativeQueryRunner(
                nativeQueryRunnerParameters.dataDirectory.toString(),
                nativeQueryRunnerParameters.serverBinary.toString(),
                nativeQueryRunnerParameters.workerCount,
                cacheMaxSize,
                useThrift,
                remoteFunctionServerUds,
                storageFormat,
                true,
                failOnNestedLoopJoin,
                isCoordinatorSidecarEnabled,
                singleNodeExecutionEnabled,
                enableRuntimeMetricsCollection,
                enableSSDCache,
                Collections.emptyMap());
    }

    // Start the remote function server. Return the UDS path used to communicate with it.
    public static String startRemoteFunctionServer(String remoteFunctionServerBinaryPath)
    {
        try {
            Path tempDirectoryPath = Files.createTempDirectory("RemoteFunctionServer");
            Path remoteFunctionServerUdsPath = tempDirectoryPath.resolve(REMOTE_FUNCTION_UDS);
            log.info("Temp directory for Remote Function Server: %s", tempDirectoryPath.toString());

            Process p = new ProcessBuilder(Paths.get(remoteFunctionServerBinaryPath).toAbsolutePath().toString(), "--uds_path", remoteFunctionServerUdsPath.toString(), "--function_prefix", REMOTE_FUNCTION_CATALOG_NAME + ".schema.")
                    .directory(tempDirectoryPath.toFile())
                    .redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.to(tempDirectoryPath.resolve("thrift_server.out").toFile()))
                    .redirectError(ProcessBuilder.Redirect.to(tempDirectoryPath.resolve("thrift_server.err").toFile()))
                    .start();
            return remoteFunctionServerUdsPath.toString();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static NativeQueryRunnerParameters getNativeQueryRunnerParameters()
    {
        Path prestoServerPath = Paths.get(getProperty("PRESTO_SERVER")
                        .orElse("_build/debug/presto_cpp/main/presto_server"))
                .toAbsolutePath();
        Path dataDirectory = Paths.get(getProperty("DATA_DIR")
                        .orElse("target/velox_data"))
                .toAbsolutePath();
        Optional<Integer> workerCount = getProperty("WORKER_COUNT").map(Integer::parseInt);

        assertTrue(Files.exists(prestoServerPath), format("Native worker binary at %s not found. Add -DPRESTO_SERVER=<path/to/presto_server> to your JVM arguments.", prestoServerPath));
        log.info("Using PRESTO_SERVER binary at %s", prestoServerPath);

        if (!Files.exists(dataDirectory)) {
            assertTrue(dataDirectory.toFile().mkdirs());
        }

        assertTrue(Files.exists(dataDirectory), format("Data directory at %s is missing. Add -DDATA_DIR=<path/to/data> to your JVM arguments to specify the path", dataDirectory));
        log.info("using DATA_DIR at %s", dataDirectory);

        return new NativeQueryRunnerParameters(prestoServerPath, dataDirectory, workerCount);
    }

    public static Optional<BiFunction<Integer, URI, Process>> getExternalWorkerLauncher(
            String catalogName,
            String prestoServerPath,
            int cacheMaxSize,
            Optional<String> remoteFunctionServerUds,
            Boolean failOnNestedLoopJoin,
            boolean isCoordinatorSidecarEnabled,
            boolean enableRuntimeMetricsCollection,
            boolean enableSsdCache)
    {
        return
                Optional.of((workerIndex, discoveryUri) -> {
                    try {
                        Path dir = Paths.get("/tmp", PrestoNativeQueryRunnerUtils.class.getSimpleName());
                        Files.createDirectories(dir);
                        Path tempDirectoryPath = Files.createTempDirectory(dir, "worker");
                        log.info("Temp directory for Worker #%d: %s", workerIndex, tempDirectoryPath.toString());

                        // Write config file - use an ephemeral port for the worker.
                        String configProperties = format("discovery.uri=%s%n" +
                                "presto.version=testversion%n" +
                                "system-memory-gb=4%n" +
                                "http-server.http.port=0%n", discoveryUri);

                        if (isCoordinatorSidecarEnabled) {
                            configProperties = format("%s%n" +
                                    "native-sidecar=true%n" +
                                    "presto.default-namespace=native.default%n", configProperties);
                        }

                        if (enableRuntimeMetricsCollection) {
                            configProperties = format("%s%n" +
                                    "runtime-metrics-collection-enabled=true%n", configProperties);
                        }

                        if (enableSsdCache) {
                            Path ssdCacheDir = Paths.get(tempDirectoryPath + "/velox-ssd-cache");
                            Files.createDirectories(ssdCacheDir);
                            configProperties = format("%s%n" +
                                    "async-cache-ssd-gb=1%n" +
                                    "async-cache-ssd-path=%s/%n", configProperties, ssdCacheDir);
                        }

                        if (remoteFunctionServerUds.isPresent()) {
                            String jsonSignaturesPath = Resources.getResource(REMOTE_FUNCTION_JSON_SIGNATURES).getFile();
                            configProperties = format("%s%n" +
                                    "remote-function-server.catalog-name=%s%n" +
                                    "remote-function-server.thrift.uds-path=%s%n" +
                                    "remote-function-server.serde=presto_page%n" +
                                    "remote-function-server.signature.files.directory.path=%s%n", configProperties, REMOTE_FUNCTION_CATALOG_NAME, remoteFunctionServerUds.get(), jsonSignaturesPath);
                        }

                        if (failOnNestedLoopJoin) {
                            configProperties = format("%s%n" + "velox-plan-validator-fail-on-nested-loop-join=true%n", configProperties);
                        }

                        Files.write(tempDirectoryPath.resolve("config.properties"), configProperties.getBytes());
                        Files.write(tempDirectoryPath.resolve("node.properties"),
                                format("node.id=%s%n" +
                                        "node.internal-address=127.0.0.1%n" +
                                        "node.environment=testing%n" +
                                        "node.location=test-location", UUID.randomUUID()).getBytes());

                        Path catalogDirectoryPath = tempDirectoryPath.resolve("catalog");
                        Files.createDirectory(catalogDirectoryPath);
                        if (cacheMaxSize > 0) {
                            Files.write(catalogDirectoryPath.resolve(format("%s.properties", catalogName)),
                                    format("connector.name=hive%n" +
                                            "cache.enabled=true%n" +
                                            "cache.max-cache-size=%s", cacheMaxSize).getBytes());
                        }
                        else {
                            Files.write(catalogDirectoryPath.resolve(format("%s.properties", catalogName)),
                                    "connector.name=hive".getBytes());
                        }
                        // Add catalog with caching always enabled.
                        Files.write(catalogDirectoryPath.resolve(format("%scached.properties", catalogName)),
                                format("connector.name=hive%n" +
                                        "cache.enabled=true%n" +
                                        "cache.max-cache-size=32").getBytes());

                        // Add a tpch catalog.
                        Files.write(catalogDirectoryPath.resolve("tpchstandard.properties"),
                                format("connector.name=tpch%n").getBytes());

                        // Disable stack trace capturing as some queries (using TRY) generate a lot of exceptions.
                        return new ProcessBuilder(prestoServerPath, "--logtostderr=1", "--v=1", "--velox_ssd_odirect=false")
                                .directory(tempDirectoryPath.toFile())
                                .redirectErrorStream(true)
                                .redirectOutput(ProcessBuilder.Redirect.to(tempDirectoryPath.resolve("worker." + workerIndex + ".out").toFile()))
                                .redirectError(ProcessBuilder.Redirect.to(tempDirectoryPath.resolve("worker." + workerIndex + ".out").toFile()))
                                .start();
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

    public static class NativeQueryRunnerParameters
    {
        public final Path serverBinary;
        public final Path dataDirectory;
        public final Optional<Integer> workerCount;

        public NativeQueryRunnerParameters(Path serverBinary, Path dataDirectory, Optional<Integer> workerCount)
        {
            this.serverBinary = requireNonNull(serverBinary, "serverBinary is null");
            this.dataDirectory = requireNonNull(dataDirectory, "dataDirectory is null");
            this.workerCount = requireNonNull(workerCount, "workerCount is null");
        }
    }

    public static void setupJsonFunctionNamespaceManager(QueryRunner queryRunner, String jsonFileName, String catalogName)
    {
        String jsonDefinitionPath = Resources.getResource(jsonFileName).getFile();
        queryRunner.installPlugin(new FunctionNamespaceManagerPlugin());
        queryRunner.loadFunctionNamespaceManager(
                JsonFileBasedFunctionNamespaceManagerFactory.NAME,
                catalogName,
                ImmutableMap.of(
                        "supported-function-languages", "CPP",
                        "function-implementation-type", "CPP",
                        "json-based-function-manager.path-to-function-definition", jsonDefinitionPath));
    }

    private static Table createHiveSymlinkTable(String databaseName, String tableName, List<Column> columns, String location)
    {
        return new Table(
                Optional.of("catalogName"),
                databaseName,
                tableName,
                "hive",
                EXTERNAL_TABLE,
                new Storage(STORAGE_FORMAT_SYMLINK_TABLE,
                        "file:" + location,
                        Optional.empty(),
                        false,
                        ImmutableMap.of(),
                        ImmutableMap.of()),
                columns,
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty());
    }
}
