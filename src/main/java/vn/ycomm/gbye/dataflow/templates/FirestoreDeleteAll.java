package vn.ycomm.gbye.dataflow.templates;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.ReadImpl;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * Dataflow template which deletes pulled Datastore Entities.
 */
public class FirestoreDeleteAll {

    /**
     * Custom PipelineOptions.
     */
    public interface FirestoreDeleteAllOptions
            extends PipelineOptions {
    }

    /**
     * Runs a pipeline which reads in Entities from datastore, passes in the JSON encoded Entities to
     * a Javascript UDF, and deletes all the Entities.
     *
     * <p>If the UDF returns value of undefined or null for a given Entity, then that Entity will not
     * be deleted.
     *
     * @param args arguments to the pipeline
     */
    public static void main(String[] args) {
        FirestoreDeleteAllOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(FirestoreDeleteAllOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply(
                        "ReadFromStatKind",
                        DatastoreIO.v1()
                                .read()
                                .withProjectId("lezhincomix-alpha")
                                .withLiteralGqlQuery("select * from __Stat_Kind__")
                                .withNamespace(""))
                .apply(
                        "StatKindToKind", ParDo.of(new DoFn<Entity, String>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) throws InvalidProtocolBufferException {
                                Entity entity = c.element();
                                assert entity != null;
                                String kind = entity.getKey().getPath(0).getName();
                                if (!kind.startsWith("__"))
                                    c.output("select __key__ from " + kind);
                            }
                        }))
                .apply("ReadFromDatastore", ReadImpl.read().withProjectId("lezhincomix-alpha"))
                .apply("EntityToKey", ParDo.of(new DoFn<Entity, Key>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws InvalidProtocolBufferException {
                        Entity entity = c.element();
                        assert entity != null;
                        c.output(c.element().getKey());
                    }
                }))
                .apply("DeleteEntity",
                        DatastoreIO.v1().deleteKey()
                                .withProjectId("lezhincomix-alpha").withRampupThrottlingDisabled()
                );

        pipeline.run();
    }
}
