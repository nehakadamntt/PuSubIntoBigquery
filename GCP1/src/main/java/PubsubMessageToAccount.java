import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;


public  class PubsubMessageToAccount extends DoFn<String, String> {

    public static TupleTag<String> parsedMessages = new TupleTag<String>(){};
    public static TupleTag<String> unparsedMessages = new TupleTag<String>(){};

    public static PCollectionTuple expand(PCollection<String> input) {
        return input
                .apply("JsonToAccount", ParDo.of(new DoFn<String, String>() {
                            @ProcessElement
                            public void processElement(ProcessContext context) {
                                String json = context.element();
                                String[] s=json.split(",");
                                if(s.length==3)
                                {
                                    if(s[0].contains("id") && s[1].contains("name") && s[2].contains("surname"))
                                    {
                                        context.output(parsedMessages,json);
                                    }
                                    else
                                    {
                                        context.output(unparsedMessages,json);
                                    }
                                }
                                else {
                                    context.output(unparsedMessages,json);
                                }
//

                            }
                        })
                        .withOutputTags(parsedMessages, TupleTagList.of(unparsedMessages)));

    }

}

}
