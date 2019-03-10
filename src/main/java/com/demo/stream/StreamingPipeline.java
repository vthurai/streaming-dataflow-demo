package com.demo.stream;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;

import com.demo.stream.config.StreamingPipelineOptions;
import com.demo.stream.dofn.MessageParser;
import com.demo.stream.dofn.WriteToBigQuery;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;

public class StreamingPipeline {
	public static Pipeline build(Pipeline p) {
		
		/*
		 * The pipeline options are constructed before the pipeline are ran with DataflowOptins and 
		 * additional options specified in StreamingPipelineOptions.  This option will feed the processes
		 * in this pipeline.
		 */
		StreamingPipelineOptions pipelineOptions = (StreamingPipelineOptions) p.getOptions();

		/*
		 * TupleTag - A TupleTag object allows Parallel Do process (ParDo) to produce more than one type
		 * of output. 
		 * 
		 * In the below example, two TupleTags are being created; parsedArticleTuple which will be the main
		 * output for the ParDo MergedProcess and failedMessageTuple which will be the output in case of any
		 * failure within the ParDo MergeProcess.  Additionally, both TupleTags contain different types being
		 * outputed (i.e KV<String, RowToInsert> vs TableRow)
		 */
		@SuppressWarnings("serial")
		TupleTag<KV<String, RowToInsert>> parsedArticleTuple = new TupleTag<KV<String, RowToInsert>>() {};
		@SuppressWarnings("serial")
		TupleTag<TableRow> failedMessageTuple = new TupleTag<TableRow>() {};		
		
		/*
		 * PubsubIO - Built in Pub/Sub connector within the Beam SDK to Read/Write byte[] messages to 
		 * Pub/Sub
		 * 
		 * The PCollections below are reading unbounded messages coming in from the Success Pubsub Topic Queue
		 * and Failure Pubsub Topic Queue produced by the Cloud Function.  To read, you must specify the
		 * subscription (fromSubcription) that is attached to the topics.  In addition, if the pubsub messages
		 * contain attributes then the readMessagesWithAttributes() method must be used vs readMessages()
		 */
		PCollection<PubsubMessage> successfulInputMessage = 
				p.apply("Read from Success Queue", PubsubIO.readMessagesWithAttributes()
						.fromSubscription(pipelineOptions.getPublishArticlesSubscription()));
		
		PCollection<PubsubMessage> failedInputMessage = 
				p.apply("Read from Failure Queue", PubsubIO.readMessagesWithAttributes()
						.fromSubscription(pipelineOptions.getFailedArticlesSubscription()));

		/*
		 * PCollectionList - Combine PCollections of the same type
		 * 
		 * Flatten - Takes the multiple PCollections that make up PCollectionList
		 * and flattens into a single PCollection
		 * 
		 * The unbounded data from both PubSub are combined into one PCollection through the
		 * use of PCollectionList and Flatten.  The data is fed into the user-defined DoFn
		 * MessageParser that will output a success message containing the Article data or a 
		 * failure message based on any errors within the Cloud Function or that DoFn.
		 */
		PCollectionTuple parsedMessages = 
				PCollectionList.of(successfulInputMessage).and(failedInputMessage)
				.apply("Combine Queue Results and Flatten",Flatten.pCollections())
				.apply("Parse Messages from Queue", ParDo.of(MessageParser.builder()
						.parsedArticleTuple(parsedArticleTuple)
						.failedMessageTuple(failedMessageTuple)
						.build())
						.withOutputTags(parsedArticleTuple, TupleTagList.of(failedMessageTuple)));

		/*
		 * Return the PCollection associated to the KV<String, RowToInsert> TupleTag.  A special coder
		 * is needed to tell Dataflow/Beam how to serialize/deserialize the custom type PCollection (generic
		 * types such as String, Integer, etc. can be inferred)
		 */
		parsedMessages.get(parsedArticleTuple)
			.setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(RowToInsert.class)))
		/*
		 * A window is incorporated to allow the pipeline the convert the unbounded data from Pub/Sub into bounded
		 * data that can be written into Big Query through the Big Query SDK.  The Window below use a Global Window
		 * (all data exist on the same window) and uses a trigger that will collect all the data in a pane up to
		 * the specified delay duration when the first element enters the pane. No late data and no data prior to
		 * this pane (i.e. discardingFiredPanes()) is collected in this window.
		 * 
		 * In order to use Windows, a aggregation function is needed. A GroupByKey is used to aggregate the Key-Value
		 * data within the PCollections into a Iterable of Value associated to the same Key.  In the user-define code,
		 * the Key for successful article messages is hardcoded to be "Article".  Therefore, all data will be aggregated
		 * under the same key. This was done to prevents concurrency issues when writing to the Big Query Table; by
		 * aggregating all the data into the same machine at fixed iteration, only a single connection will exist to
		 * the Big Query table to write the data.
		 */
			.apply(Window.<KV<String, RowToInsert>>into(new GlobalWindows())
					.triggering(Repeatedly.forever(AfterProcessingTime
							.pastFirstElementInPane()
							.plusDelayOf(Duration.standardSeconds(pipelineOptions.getPublishArticleWindow()))))
					.withAllowedLateness(Duration.ZERO)
					.discardingFiredPanes())
			.apply(GroupByKey.create())
		/*
		 * WriteToBigQuery is a user-defined class that takes an Iterable of RowToInsert and directly uploads
		 * it to the specified table using the Google Big Query SDK.
		 */
			.apply("Append Articles to BigQuery", ParDo.of(WriteToBigQuery.builder()
					.projectId(pipelineOptions.getProject())
					.datasetId(pipelineOptions.getDatasetId())
					.tableName(pipelineOptions.getArticleTableId())
					.build()));

		/*
		 * For the failure messages, the BigQuery IO cConnector, as part of the Dataflow SDK, has been used to
		 * demonstrate an alternate method to write data.  Unlike using the Big Query SDK, the BigQueryIO handles
		 * the multi-write concurrency issues posed in the above code. Many popular Google Cloud services such as
		 * Cloud Storage, Pub/Sub, and Datastore have built-in IO connectors as pard of the Dataflow SDK.
		 * 
		 * The BigQueryIO connector requires the table path to be displayed as:
		 * 		[project-id]:[dataset-id].[table-id]
		 * In addition, the BigQueryIO options is set to only append incoming data to the table and to never
		 * create a new table if the table path provided doesn't exist (an error will show up)
		 */
		parsedMessages.get(failedMessageTuple)
			.apply(BigQueryIO.writeTableRows()
				.to(String.format("%s:%s.%s", pipelineOptions.getProject(), pipelineOptions.getDatasetId(), pipelineOptions.getErrorTableId()))
				.withLoadJobProjectId(pipelineOptions.getProject())
			    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
		
		return p;
	}
}
