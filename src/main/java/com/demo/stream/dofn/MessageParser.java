package com.demo.stream.dofn;

import java.util.HashMap;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Strings;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/*
 * When using TupleTags, the main TupleTag Output must be the ouput referenced for the 
 * DoFn.  In the below, the DoFn shows an input of PubsubMessage and an output of
 * KV<String, RowToInsert>.  The failure TupleTag is an additional output and is not
 * referenced in the header.
 */
@Slf4j
public class MessageParser extends DoFn<PubsubMessage, KV<String, RowToInsert>> {
	
	private static final long serialVersionUID = -2154622492105976485L;
	private TupleTag<KV<String, RowToInsert>> parsedArticleTuple;
	private TupleTag<TableRow> failedMessageTuple;
	private ObjectMapper objectMapper;
	
	/*
	 * TupleTags and Side Inputs must be passed in the User-Defined DoFn to be able to use it
	 * within the process.  In this case, the success and failed TupleTag have been passed in 
	 * and parsed in the local attributes
	 */
	@Builder
	public MessageParser(TupleTag<KV<String, RowToInsert>> parsedArticleTuple,
			TupleTag<TableRow> failedMessageTuple) {
		this.parsedArticleTuple = parsedArticleTuple;
		this.failedMessageTuple = failedMessageTuple;
	}
	
	@Setup
	public void setup() {
		objectMapper = new ObjectMapper();
	}
	
	@AllArgsConstructor
	@Data
	static class Process{
		private String serviceName;
		private String step;
	}	
	
	/*
	 * The processElement class contains two arguments: the ProcessContext and MultiOutputReciever. The
	 * ProcessContext contains the main information of the process during run time such as the input,
	 * the main output, side input, etc. The MultiOutputReciever enables the process to output to two or
	 * more different receiver (i.e. the TupleTag Objects)
	 */
	@ProcessElement
	public void processElement (ProcessContext c, MultiOutputReceiver out) {
		/*
		 * Read the input message and parse it into the object pubsubMessage
		 */
		PubsubMessage pubsubMessage = c.element();
		try {
			/*
			 * If the status attribute in the pubsub message was success, use the Object Mapper class, part of the
			 * Jackson library, to parse the JSON message contain in the pubsub payload into a HashMap object, which
			 * is directly fed into the RowToInsert object.
			 */
			if(pubsubMessage.getAttribute("status").equals("success")) {
				RowToInsert row = RowToInsert.of(objectMapper.readValue(c.element().getPayload(), HashMap.class));
				out.get(parsedArticleTuple).output(KV.of("Article", row));
			}
			/*
			 * Else if the status attribute was a failure (only two options from the Cloud Function), map the 
			 * payload message into the TableRow ojbect.
			 */
			else {
				TableRow failedCFMessage = objectMapper.readValue(c.element().getPayload(), TableRow.class);
				out.get(failedMessageTuple).output(failedCFMessage);
			}
		/*
		 * Should a failure occur during this process, catch the error and parse it into a new TableRow object
		 * to be sent through the failure Tuple Tag.
		 * 
		 * Notes: Should an error occur within a Streaming pipeline, the pipeline will retry the code indefinetly.
		 * Shoud the failure be unrecoverable, the pipeline will become unstable and require a manual shutdown.
		 */
		} catch(Exception e) {
			log.error("Failure at Message Parser", e);
			String errorMessage = (Strings.isNullOrEmpty(e.getMessage())) ? "Unable to parse error" : e.getMessage();
			TableRow failedDFMessage = new TableRow()
					.set("status", "error")
					.set("code", "NA")
					.set("message", errorMessage)
					.set("timeStamp", DateTime.now().toString())
					.set("process", new Process("Dataflow", "MessageParser"));
			out.get(failedMessageTuple).output(failedDFMessage);
		}
	}
}


