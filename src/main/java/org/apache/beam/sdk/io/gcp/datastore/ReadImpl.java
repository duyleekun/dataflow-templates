package org.apache.beam.sdk.io.gcp.datastore;

import com.google.auto.value.AutoValue;
import com.google.datastore.v1.*;
import com.google.datastore.v1.client.Datastore;
import com.google.datastore.v1.client.DatastoreException;
import com.google.datastore.v1.client.QuerySplitter;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import org.apache.beam.runners.core.metrics.GcpResourceIdentifiers;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.ServiceCallMetric;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.google.datastore.v1.PropertyFilter.Operator.EQUAL;
import static com.google.datastore.v1.PropertyOrder.Direction.DESCENDING;
import static com.google.datastore.v1.QueryResultBatch.MoreResultsType.NOT_FINISHED;
import static com.google.datastore.v1.client.DatastoreHelper.*;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Verify.verify;
@AutoValue
public abstract class ReadImpl extends PTransform<PCollection<String>, PCollection<Entity>> {
    private static final Set<Code> NON_RETRYABLE_ERRORS =
            ImmutableSet.of(
                    Code.FAILED_PRECONDITION,
                    Code.INVALID_ARGUMENT,
                    Code.PERMISSION_DENIED,
                    Code.UNAUTHENTICATED);

    private static final Logger LOG = LoggerFactory.getLogger(ReadImpl.class);

    /** An upper bound on the number of splits for a query. */
    public static final int NUM_QUERY_SPLITS_MAX = 50000;

    /** A lower bound on the number of splits for a query. */
    static final int NUM_QUERY_SPLITS_MIN = 12;

    /** Default bundle size of 64MB. */
    static final long DEFAULT_BUNDLE_SIZE_BYTES = 64L * 1024L * 1024L;

    /**
     * Maximum number of results to request per query.
     *
     * <p>Must be set, or it may result in an I/O error when querying Cloud Datastore.
     */
    static final int QUERY_BATCH_LIMIT = 500;

    public abstract @Nullable ValueProvider<String> getProjectId();

    public abstract @Nullable ValueProvider<String> getNamespace();

    public abstract int getNumQuerySplits();

    public abstract @Nullable String getLocalhost();

    public abstract @Nullable Instant getReadTime();

    @Override
    public abstract String toString();

    abstract Builder toBuilder();

    public static ReadImpl read() {
        return new AutoValue_ReadImpl.Builder().setNumQuerySplits(0).build();
    }

    @AutoValue.Builder
    abstract static public class Builder {
        abstract Builder setProjectId(ValueProvider<String> projectId);

        abstract Builder setNamespace(ValueProvider<String> namespace);

        abstract Builder setNumQuerySplits(int numQuerySplits);

        abstract Builder setLocalhost(String localhost);

        abstract Builder setReadTime(Instant readTime);

        abstract ReadImpl build();
    }

    /**
     * Computes the number of splits to be performed on the given query by querying the estimated
     * size from Cloud Datastore.
     */
    static int getEstimatedNumSplits(
            Datastore datastore, Query query, @Nullable String namespace, @Nullable Instant readTime) {
        int numSplits;
        try {
            long estimatedSizeBytes = getEstimatedSizeBytes(datastore, query, namespace, readTime);
            LOG.info("Estimated size bytes for the query is: {}", estimatedSizeBytes);
            numSplits =
                    (int)
                            Math.min(
                                    NUM_QUERY_SPLITS_MAX,
                                    Math.round(((double) estimatedSizeBytes) / DEFAULT_BUNDLE_SIZE_BYTES));
        } catch (Exception e) {
            LOG.warn("Failed the fetch estimatedSizeBytes for query: {}", query, e);
            // Fallback in case estimated size is unavailable.
            numSplits = NUM_QUERY_SPLITS_MIN;
        }
        return Math.max(numSplits, NUM_QUERY_SPLITS_MIN);
    }

    /**
     * Cloud Datastore system tables with statistics are periodically updated. This method fetches
     * the latest timestamp (in microseconds) of statistics update using the {@code __Stat_Total__}
     * table.
     */
    private static long queryLatestStatisticsTimestamp(
            Datastore datastore, @Nullable String namespace, @Nullable Instant readTime)
            throws DatastoreException {
        Query.Builder query = Query.newBuilder();
        // Note: namespace either being null or empty represents the default namespace, in which
        // case we treat it as not provided by the user.
        if (Strings.isNullOrEmpty(namespace)) {
            query.addKindBuilder().setName("__Stat_Total__");
        } else {
            query.addKindBuilder().setName("__Stat_Ns_Total__");
        }
        query.addOrder(makeOrder("timestamp", DESCENDING));
        query.setLimit(Int32Value.newBuilder().setValue(1));
        RunQueryRequest request = makeRequest(query.build(), namespace, readTime);

        RunQueryResponse response = datastore.runQuery(request);
        QueryResultBatch batch = response.getBatch();
        if (batch.getEntityResultsCount() == 0) {
            throw new NoSuchElementException("Datastore total statistics unavailable");
        }
        Entity entity = batch.getEntityResults(0).getEntity();
        return entity.getProperties().get("timestamp").getTimestampValue().getSeconds() * 1000000;
    }

    /**
     * Retrieve latest table statistics for a given kind, namespace, and datastore. If the Read has
     * readTime specified, the latest statistics at or before readTime is retrieved.
     */
    private static Entity getLatestTableStats(
            String ourKind, @Nullable String namespace, Datastore datastore, @Nullable Instant readTime)
            throws DatastoreException {
        long latestTimestamp = queryLatestStatisticsTimestamp(datastore, namespace, readTime);
        LOG.info("Latest stats timestamp for kind {} is {}", ourKind, latestTimestamp);

        Query.Builder queryBuilder = Query.newBuilder();
        if (Strings.isNullOrEmpty(namespace)) {
            queryBuilder.addKindBuilder().setName("__Stat_Kind__");
        } else {
            queryBuilder.addKindBuilder().setName("__Stat_Ns_Kind__");
        }

        queryBuilder.setFilter(
                makeAndFilter(
                        makeFilter("kind_name", EQUAL, makeValue(ourKind).build()).build(),
                        makeFilter("timestamp", EQUAL, makeValue(latestTimestamp).build()).build()));

        RunQueryRequest request = makeRequest(queryBuilder.build(), namespace, readTime);

        long now = System.currentTimeMillis();
        RunQueryResponse response = datastore.runQuery(request);
        LOG.debug("Query for per-kind statistics took {}ms", System.currentTimeMillis() - now);

        QueryResultBatch batch = response.getBatch();
        if (batch.getEntityResultsCount() == 0) {
            throw new NoSuchElementException(
                    "Datastore statistics for kind " + ourKind + " unavailable");
        }
        return batch.getEntityResults(0).getEntity();
    }

    /**
     * Get the estimated size of the data returned by the given query.
     *
     * <p>Cloud Datastore provides no way to get a good estimate of how large the result of a query
     * entity kind being queried, using the __Stat_Kind__ system table, assuming exactly 1 kind is
     * specified in the query.
     *
     * <p>See https://cloud.google.com/datastore/docs/concepts/stats.
     */
    static long getEstimatedSizeBytes(
            Datastore datastore, Query query, @Nullable String namespace, @Nullable Instant readTime)
            throws DatastoreException {
        String ourKind = query.getKind(0).getName();
        Entity entity = getLatestTableStats(ourKind, namespace, datastore, readTime);
        return entity.getProperties().get("entity_bytes").getIntegerValue();
    }

    private static PartitionId.Builder forNamespace(@Nullable String namespace) {
        PartitionId.Builder partitionBuilder = PartitionId.newBuilder();
        // Namespace either being null or empty represents the default namespace.
        // Datastore Client libraries expect users to not set the namespace proto field in
        // either of these cases.
        if (!Strings.isNullOrEmpty(namespace)) {
            partitionBuilder.setNamespaceId(namespace);
        }
        return partitionBuilder;
    }

    /**
     * Builds a {@link RunQueryRequest} from the {@code query} and {@code namespace}, optionally at
     * the requested {@code readTime}.
     */
    static RunQueryRequest makeRequest(
            Query query, @Nullable String namespace, @Nullable Instant readTime) {
        RunQueryRequest.Builder request =
                RunQueryRequest.newBuilder().setQuery(query).setPartitionId(forNamespace(namespace));
        if (readTime != null) {
            Timestamp readTimeProto = Timestamps.fromMillis(readTime.getMillis());
            request.setReadOptions(ReadOptions.newBuilder().setReadTime(readTimeProto).build());
        }
        return request.build();
    }

    @VisibleForTesting
    /**
     * Builds a {@link RunQueryRequest} from the {@code GqlQuery} and {@code namespace}, optionally
     * at the requested {@code readTime}.
     */
    static RunQueryRequest makeRequest(
            GqlQuery gqlQuery, @Nullable String namespace, @Nullable Instant readTime) {
        RunQueryRequest.Builder request =
                RunQueryRequest.newBuilder()
                        .setGqlQuery(gqlQuery)
                        .setPartitionId(forNamespace(namespace));
        if (readTime != null) {
            Timestamp readTimeProto = Timestamps.fromMillis(readTime.getMillis());
            request.setReadOptions(ReadOptions.newBuilder().setReadTime(readTimeProto).build());
        }

        return request.build();
    }

    /**
     * A helper function to get the split queries, taking into account the optional {@code
     * namespace}.
     */
    private static List<Query> splitQuery(
            Query query,
            @Nullable String namespace,
            Datastore datastore,
            QuerySplitter querySplitter,
            int numSplits,
            @Nullable Instant readTime)
            throws DatastoreException {
        // If namespace is set, include it in the split request so splits are calculated accordingly.
        PartitionId partitionId = forNamespace(namespace).build();
        if (readTime != null) {
            Timestamp readTimeProto = Timestamps.fromMillis(readTime.getMillis());
            return querySplitter.getSplits(query, partitionId, numSplits, datastore, readTimeProto);
        }
        return querySplitter.getSplits(query, partitionId, numSplits, datastore);
    }

    /**
     * Translates a Cloud Datastore gql query string to {@link Query}.
     *
     * <p>Currently, the only way to translate a gql query string to a Query is to run the query
     * against Cloud Datastore and extract the {@code Query} from the response. To prevent reading
     * any data, we set the {@code LIMIT} to 0 but if the gql query already has a limit set, we
     * catch the exception with {@code INVALID_ARGUMENT} error code and retry the translation
     * without the zero limit.
     *
     * <p>Note: This may result in reading actual data from Cloud Datastore but the service has a
     * cap on the number of entities returned for a single rpc request, so this should not be a
     * problem in practice.
     */
    @VisibleForTesting
    static Query translateGqlQueryWithLimitCheck(
            String gql, Datastore datastore, String namespace, @Nullable Instant readTime)
            throws DatastoreException {
        String gqlQueryWithZeroLimit = gql + " LIMIT 0";
        try {
            Query translatedQuery =
                    translateGqlQuery(gqlQueryWithZeroLimit, datastore, namespace, readTime);
            // Clear the limit that we set.
            return translatedQuery.toBuilder().clearLimit().build();
        } catch (DatastoreException e) {
            // Note: There is no specific error code or message to detect if the query already has a
            // limit, so we just check for INVALID_ARGUMENT and assume that that the query might have
            // a limit already set.
            if (e.getCode() == Code.INVALID_ARGUMENT) {
                LOG.warn("Failed to translate Gql query '{}': {}", gqlQueryWithZeroLimit, e.getMessage());
                LOG.warn("User query might have a limit already set, so trying without zero limit");
                // Retry without the zero limit.
                return translateGqlQuery(gql, datastore, namespace, readTime);
            } else {
                throw e;
            }
        }
    }

    /** Translates a gql query string to {@link Query}. */
    private static Query translateGqlQuery(
            String gql, Datastore datastore, String namespace, @Nullable Instant readTime)
            throws DatastoreException {
        GqlQuery gqlQuery = GqlQuery.newBuilder().setQueryString(gql).setAllowLiterals(true).build();
        RunQueryRequest req = makeRequest(gqlQuery, namespace, readTime);
        return datastore.runQuery(req).getQuery();
    }

    /**
     * Returns a new {@link ReadImpl} that reads from the Cloud Datastore for the specified
     * project.
     */
    public ReadImpl withProjectId(String projectId) {
        checkArgument(projectId != null, "projectId can not be null");
        return toBuilder().setProjectId(ValueProvider.StaticValueProvider.of(projectId)).build();
    }

    /** Same as {@link ReadImpl#withProjectId(String)} but with a {@link ValueProvider}. */
    public ReadImpl withProjectId(ValueProvider<String> projectId) {
        checkArgument(projectId != null, "projectId can not be null");
        return toBuilder().setProjectId(projectId).build();
    }

    /** Returns a new {@link ReadImpl} that reads from the given namespace. */
    public ReadImpl withNamespace(String namespace) {
        return toBuilder().setNamespace(ValueProvider.StaticValueProvider.of(namespace)).build();
    }

    /** Same as {@link ReadImpl#withNamespace(String)} but with a {@link ValueProvider}. */
    public ReadImpl withNamespace(ValueProvider<String> namespace) {
        return toBuilder().setNamespace(namespace).build();
    }

    /**
     * Returns a new {@link ReadImpl} that reads by splitting the given {@code query} into
     * {@code numQuerySplits}.
     *
     * <p>The semantics for the query splitting is defined below:
     *
     * <ul>
     *   <li>Any value less than or equal to 0 will be ignored, and the number of splits will be
     *       chosen dynamically at runtime based on the query data size.
     *   <li>Any value greater than {@link ReadImpl#NUM_QUERY_SPLITS_MAX} will be capped at {@code
     *       NUM_QUERY_SPLITS_MAX}.
     *   <li>If the {@code query} has a user limit set, then {@code numQuerySplits} will be ignored
     *       and no split will be performed.
     *   <li>Under certain cases Cloud Datastore is unable to split query to the requested number of
     *       splits. In such cases we just use whatever the Cloud Datastore returns.
     * </ul>
     */
    public ReadImpl withNumQuerySplits(int numQuerySplits) {
        return toBuilder()
                .setNumQuerySplits(Math.min(Math.max(numQuerySplits, 0), NUM_QUERY_SPLITS_MAX))
                .build();
    }

    /**
     * Returns a new {@link ReadImpl} that reads from a Datastore Emulator running at the
     * given localhost address.
     */
    public ReadImpl withLocalhost(String localhost) {
        return toBuilder().setLocalhost(localhost).build();
    }

    /** Returns a new {@link ReadImpl} that reads at the specified {@code readTime}. */
    public ReadImpl withReadTime(Instant readTime) {
        return toBuilder().setReadTime(readTime).build();
    }

    /** Returns Number of entities available for reading. */
    public long getNumEntities(
            PipelineOptions options, String ourKind, @Nullable String namespace) {
        try {
            V1Options v1Options = V1Options.from(getProjectId(), getNamespace(), getLocalhost());
            DatastoreV1.V1DatastoreFactory datastoreFactory = new DatastoreV1.V1DatastoreFactory();
            Datastore datastore =
                    datastoreFactory.getDatastore(
                            options, v1Options.getProjectId(), v1Options.getLocalhost());

            Entity entity = getLatestTableStats(ourKind, namespace, datastore, getReadTime());
            return entity.getProperties().get("count").getIntegerValue();
        } catch (Exception e) {
            return -1;
        }
    }

    @Override
    public PCollection<Entity> expand(PCollection<String> input) {
        checkArgument(getProjectId() != null, "projectId provider cannot be null");
        if (getProjectId().isAccessible()) {
            checkArgument(getProjectId().get() != null, "projectId cannot be null");
        }

        V1Options v1Options = V1Options.from(getProjectId(), getNamespace(), getLocalhost());

        /*
         * This composite transform involves the following steps:
         *   1. Create a singleton of the user provided {@code query} or if {@code gqlQuery} is
         *   provided apply a {@link ParDo} that translates the {@code gqlQuery} into a {@code query}.
         *
         *   2. A {@link ParDo} splits the resulting query into {@code numQuerySplits} and
         *   assign each split query a unique {@code Integer} as the key. The resulting output is
         *   of the type {@code PCollection<KV<Integer, Query>>}.
         *
         *   If the value of {@code numQuerySplits} is less than or equal to 0, then the number of
         *   splits will be computed dynamically based on the size of the data for the {@code query}.
         *
         *   3. The resulting {@code PCollection} is sharded using a {@link GroupByKey} operation. The
         *   queries are extracted from they {@code KV<Integer, Iterable<Query>>} and flattened to
         *   output a {@code PCollection<Query>}.
         *
         *   4. In the third step, a {@code ParDo} reads entities for each query and outputs
         *   a {@code PCollection<Entity>}.
         */

        return input
                .apply(ParDo.of(new GqlQueryTranslateFn(v1Options, getReadTime())))
                .apply("Split", ParDo.of(new SplitQueryFn(v1Options, getNumQuerySplits(), getReadTime())))
                .apply("Reshuffle", Reshuffle.viaRandomKey())
                .apply("Read", ParDo.of(new ReadFn(v1Options, getReadTime())));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        builder
                .addIfNotNull(DisplayData.item("projectId", getProjectId()).withLabel("ProjectId"))
                .addIfNotNull(DisplayData.item("namespace", getNamespace()).withLabel("Namespace"))
                .addIfNotNull(DisplayData.item("readTime", getReadTime()).withLabel("ReadTime"));
    }

    @VisibleForTesting
    static class V1Options implements HasDisplayData, Serializable {
        private final ValueProvider<String> project;
        private final @Nullable ValueProvider<String> namespace;
        private final @Nullable String localhost;

        private V1Options(
                ValueProvider<String> project, ValueProvider<String> namespace, String localhost) {
            this.project = project;
            this.namespace = namespace;
            this.localhost = localhost;
        }

        public static V1Options from(String projectId, String namespace, String localhost) {
            return from(
                    ValueProvider.StaticValueProvider.of(projectId), ValueProvider.StaticValueProvider.of(namespace), localhost);
        }

        public static V1Options from(
                ValueProvider<String> project, ValueProvider<String> namespace, String localhost) {
            return new V1Options(project, namespace, localhost);
        }

        public String getProjectId() {
            return project.get();
        }

        public @Nullable String getNamespace() {
            return namespace == null ? null : namespace.get();
        }

        public ValueProvider<String> getProjectValueProvider() {
            return project;
        }

        public @Nullable ValueProvider<String> getNamespaceValueProvider() {
            return namespace;
        }

        public @Nullable String getLocalhost() {
            return localhost;
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            builder
                    .addIfNotNull(
                            DisplayData.item("projectId", getProjectValueProvider()).withLabel("ProjectId"))
                    .addIfNotNull(
                            DisplayData.item("namespace", getNamespaceValueProvider()).withLabel("Namespace"));
        }
    }

    /** A DoFn that translates a Cloud Datastore gql query string to {@code Query}. */
    static class GqlQueryTranslateFn extends DoFn<String, Query> {
        private final V1Options v1Options;
        private final @Nullable Instant readTime;
        private transient Datastore datastore;
        private final DatastoreV1.V1DatastoreFactory datastoreFactory;

        GqlQueryTranslateFn(V1Options options) {
            this(options, null, new DatastoreV1.V1DatastoreFactory());
        }

        GqlQueryTranslateFn(V1Options options, @Nullable Instant readTime) {
            this(options, readTime, new DatastoreV1.V1DatastoreFactory());
        }

        GqlQueryTranslateFn(
                V1Options options, @Nullable Instant readTime, DatastoreV1.V1DatastoreFactory datastoreFactory) {
            this.v1Options = options;
            this.readTime = readTime;
            this.datastoreFactory = datastoreFactory;
        }

        @StartBundle
        public void startBundle(StartBundleContext c) throws Exception {
            datastore =
                    datastoreFactory.getDatastore(
                            c.getPipelineOptions(), v1Options.getProjectId(), v1Options.getLocalhost());
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            String gqlQuery = c.element();
            LOG.info("User query: '{}'", gqlQuery);
            Query query =
                    translateGqlQueryWithLimitCheck(
                            gqlQuery, datastore, v1Options.getNamespace(), readTime);
            LOG.info("User gql query translated to Query({})", query);
            c.output(query);
        }
    }

    /**
     * A {@link DoFn} that splits a given query into multiple sub-queries, assigns them unique keys
     * and outputs them as {@link KV}.
     */
    @VisibleForTesting
    static class SplitQueryFn extends DoFn<Query, Query> {
        private final V1Options options;
        // number of splits to make for a given query
        private final int numSplits;
        // time from which to run the queries
        private final @Nullable Instant readTime;

        private final DatastoreV1.V1DatastoreFactory datastoreFactory;
        // Datastore client
        private transient Datastore datastore;
        // Query splitter
        private transient QuerySplitter querySplitter;

        public SplitQueryFn(V1Options options, int numSplits) {
            this(options, numSplits, null, new DatastoreV1.V1DatastoreFactory());
        }

        public SplitQueryFn(V1Options options, int numSplits, @Nullable Instant readTime) {
            this(options, numSplits, readTime, new DatastoreV1.V1DatastoreFactory());
        }

        @VisibleForTesting
        SplitQueryFn(
                V1Options options,
                int numSplits,
                @Nullable Instant readTime,
                DatastoreV1.V1DatastoreFactory datastoreFactory) {
            this.options = options;
            this.numSplits = numSplits;
            this.datastoreFactory = datastoreFactory;
            this.readTime = readTime;
        }

        @StartBundle
        public void startBundle(StartBundleContext c) throws Exception {
            datastore =
                    datastoreFactory.getDatastore(
                            c.getPipelineOptions(), options.getProjectId(), options.getLocalhost());
            querySplitter = datastoreFactory.getQuerySplitter();
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            Query query = c.element();

            // If query has a user set limit, then do not split.
            if (query.hasLimit()) {
                c.output(query);
                return;
            }

            int estimatedNumSplits;
            // Compute the estimated numSplits if numSplits is not specified by the user.
            if (numSplits <= 0) {
                estimatedNumSplits =
                        getEstimatedNumSplits(datastore, query, options.getNamespace(), readTime);
            } else {
                estimatedNumSplits = numSplits;
            }

            LOG.info("Splitting the query into {} splits", estimatedNumSplits);
            List<Query> querySplits;
            try {
                querySplits =
                        splitQuery(
                                query,
                                options.getNamespace(),
                                datastore,
                                querySplitter,
                                estimatedNumSplits,
                                readTime);
            } catch (Exception e) {
                LOG.warn("Unable to parallelize the given query: {}", query, e);
                querySplits = ImmutableList.of(query);
            }

            // assign unique keys to query splits.
            for (Query subquery : querySplits) {
                c.output(subquery);
            }
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder.include("options", options);
            if (numSplits > 0) {
                builder.add(
                        DisplayData.item("numQuerySplits", numSplits)
                                .withLabel("Requested number of Query splits"));
            }
            builder.addIfNotNull(DisplayData.item("readTime", readTime).withLabel("ReadTime"));
        }
    }

    /** A {@link DoFn} that reads entities from Cloud Datastore for each query. */
    @VisibleForTesting
    static class ReadFn extends DoFn<Query, Entity> {
        private final V1Options options;
        private final @Nullable Instant readTime;
        private final DatastoreV1.V1DatastoreFactory datastoreFactory;
        // Datastore client
        private transient Datastore datastore;
        private final Counter rpcErrors =
                Metrics.counter(DatastoreV1.DatastoreWriterFn.class, "datastoreRpcErrors");
        private final Counter rpcSuccesses =
                Metrics.counter(DatastoreV1.DatastoreWriterFn.class, "datastoreRpcSuccesses");
        private static final int MAX_RETRIES = 5;
        private static final FluentBackoff RUNQUERY_BACKOFF =
                FluentBackoff.DEFAULT
                        .withMaxRetries(MAX_RETRIES)
                        .withInitialBackoff(Duration.standardSeconds(5));

        public ReadFn(V1Options options) {
            this(options, null, new DatastoreV1.V1DatastoreFactory());
        }

        public ReadFn(V1Options options, @Nullable Instant readTime) {
            this(options, readTime, new DatastoreV1.V1DatastoreFactory());
        }

        @VisibleForTesting
        ReadFn(V1Options options, @Nullable Instant readTime, DatastoreV1.V1DatastoreFactory datastoreFactory) {
            this.options = options;
            this.readTime = readTime;
            this.datastoreFactory = datastoreFactory;
        }

        @StartBundle
        public void startBundle(StartBundleContext c) throws Exception {
            datastore =
                    datastoreFactory.getDatastore(
                            c.getPipelineOptions(), options.getProjectId(), options.getLocalhost());
        }

        private RunQueryResponse runQueryWithRetries(RunQueryRequest request) throws Exception {
            Sleeper sleeper = Sleeper.DEFAULT;
            BackOff backoff = RUNQUERY_BACKOFF.backoff();
            while (true) {
                HashMap<String, String> baseLabels = new HashMap<>();
                baseLabels.put(MonitoringInfoConstants.Labels.PTRANSFORM, "");
                baseLabels.put(MonitoringInfoConstants.Labels.SERVICE, "Datastore");
                baseLabels.put(MonitoringInfoConstants.Labels.METHOD, "BatchDatastoreRead");
                baseLabels.put(
                        MonitoringInfoConstants.Labels.RESOURCE,
                        GcpResourceIdentifiers.datastoreResource(
                                options.getProjectId(), options.getNamespace()));
                baseLabels.put(MonitoringInfoConstants.Labels.DATASTORE_PROJECT, options.getProjectId());
                baseLabels.put(
                        MonitoringInfoConstants.Labels.DATASTORE_NAMESPACE,
                        String.valueOf(options.getNamespace()));
                ServiceCallMetric serviceCallMetric =
                        new ServiceCallMetric(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, baseLabels);
                try {
                    RunQueryResponse response = datastore.runQuery(request);
                    serviceCallMetric.call("ok");
                    rpcSuccesses.inc();
                    return response;
                } catch (DatastoreException exception) {
                    rpcErrors.inc();
                    serviceCallMetric.call(exception.getCode().getNumber());

                    if (NON_RETRYABLE_ERRORS.contains(exception.getCode())) {
                        throw exception;
                    }
                    if (!BackOffUtils.next(sleeper, backoff)) {
                        LOG.error("Aborting after {} retries.", MAX_RETRIES);
                        throw exception;
                    }
                }
            }
        }

        /** Read and output entities for the given query. */
        @ProcessElement
        public void processElement(ProcessContext context) throws Exception {
            Query query = context.element();
            String namespace = options.getNamespace();
            int userLimit = query.hasLimit() ? query.getLimit().getValue() : Integer.MAX_VALUE;

            boolean moreResults = true;
            QueryResultBatch currentBatch = null;

            while (moreResults) {
                Query.Builder queryBuilder = query.toBuilder();
                queryBuilder.setLimit(
                        Int32Value.newBuilder().setValue(Math.min(userLimit, QUERY_BATCH_LIMIT)));

                if (currentBatch != null && !currentBatch.getEndCursor().isEmpty()) {
                    queryBuilder.setStartCursor(currentBatch.getEndCursor());
                }

                RunQueryRequest request = makeRequest(queryBuilder.build(), namespace, readTime);
                RunQueryResponse response = runQueryWithRetries(request);

                currentBatch = response.getBatch();

                // MORE_RESULTS_AFTER_LIMIT is not implemented yet:
                // https://groups.google.com/forum/#!topic/gcd-discuss/iNs6M1jA2Vw, so
                // use result count to determine if more results might exist.
                int numFetch = currentBatch.getEntityResultsCount();
                if (query.hasLimit()) {
                    verify(
                            userLimit >= numFetch,
                            "Expected userLimit %s >= numFetch %s, because query limit %s must be <= userLimit",
                            userLimit,
                            numFetch,
                            query.getLimit());
                    userLimit -= numFetch;
                }

                // output all the entities from the current batch.
                for (EntityResult entityResult : currentBatch.getEntityResultsList()) {
                    context.output(entityResult.getEntity());
                }

                // Check if we have more entities to be read.
                moreResults =
                        // User-limit does not exist (so userLimit == MAX_VALUE) and/or has not been satisfied
                        (userLimit > 0)
                                // All indications from the API are that there are/may be more results.
                                && ((numFetch == QUERY_BATCH_LIMIT)
                                || (currentBatch.getMoreResults() == NOT_FINISHED));
            }
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder.include("options", options);
            builder.addIfNotNull(DisplayData.item("readTime", readTime).withLabel("ReadTime"));
        }
    }


}
