import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

public class PubSubToBQ {
    static final TupleTag<Account> parsedMessages = new TupleTag<Account>() {
    };
    static final TupleTag<String> unparsedMessages = new TupleTag<String>() {
    };
    public interface MyOptions extends DataflowPipelineOptions {

        @Description("BigQuery table name")
        String getOutputTableName();
        void setOutputTableName(String outputTableName);

        @Description("PubSub Subscription")
        String getSubscription();
        void setSubscription(String subscription);
    }


    public static final Schema account=Schema.
            builder()
            .addInt16Field("id")
            .addStringField("name")
            .addStringField("surname")
            .build();


    public static void main(String[] args) {
        System.out.println("Hello");


        //Getting Options mentioned in command
        Main.MyOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(Main.MyOptions.class);

       run(options);



    }
    public static PipelineResult run(Main.MyOptions options)
    {
        Pipeline p=Pipeline.create(options);

        PCollection<String> message=p.apply("GetDataFromPUBSub", PubsubIO.readStrings().fromSubscription(options.getSubscription()));
        PCollectionTuple transformOut =PubsubMessageToAccount.expand(message);
        PCollection<String> accountCollection=transformOut.get(PubsubMessageToAccount.parsedMessages);
        PCollection<String> unparsedCollection=transformOut.get(PubsubMessageToAccount.unparsedMessages);

        accountCollection.apply("JSONTOROW", JsonToRow.withSchema(account)).
                apply("WriteToBigqury", BigQueryIO.<Row>write().to(options.getOutputTableName()).useBeamSchema()
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        unparsedCollection.apply("write to dlq",PubsubIO.writeStrings().to(""));
        return p.run(); 
    }

}
