package com.demo.stream.dofn;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.collections4.IterableUtils;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.InsertAllResponse;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WriteToBigQuery extends DoFn<KV<String, Iterable<RowToInsert>>, KV<String, Integer>>{

	private static final long serialVersionUID = -5468097734258588448L;
	private BigQuery bigquery;
	private String projectId;
	private String datasetId;
	private String tableName;
	
	/*
	 * Passing in arguments to WriteToBigQuery DoFn that point to the tale reference during
	 * compiler/build time
	 */
	@Builder
	public WriteToBigQuery(String projectId,
			String datasetId,
			String tableName) {
		this.projectId = projectId;
		this.datasetId = datasetId;
		this.tableName = tableName;
	}
	
	/*
	 * Setup annotation/method allows objects that should be reused for all threads to be initialized (i.e. 
	 * the objects are transient). For example, it is not needed to re-initialize the BigQuery connection
	 * everytime a new element enters this DoFn. This will be costly in both time to initialize and memory.
	 * 
	 * Note: Be careful when using the set-up annotation/method. Since the object are transient, objects that
	 * retain memory may contain data from other threads of the DoFn leading to issues.  
	 */
	@Setup
	public void setup() {
		bigquery = BigQueryOptions.getDefaultInstance().getService();
	}
	
	@ProcessElement
	public void processElement (ProcessContext c) {
		try {
			/*
			 * IterableUtils is a static library part of Apache Commons
			 */
			int size = IterableUtils.size(c.element().getValue());
			/*
			 * Creating the TableId object using the passed-in arguments from the constructor
			 */
			TableId tableId = TableId.of(projectId, datasetId, tableName);
			/*
			 * Using the BigQuery Interface object to retrieve the table using the table Id and directly
			 * inserting the Iterable of RowToInsert objects (i.e appending the table with muliple rows).
			 * Options are set to not skip rows if they are invalid (e.g. null value when the schema forces
			 * non-null entries) or unknown values.
			 */
			InsertAllResponse response = bigquery.getTable(tableId).insert(c.element().getValue(), false, false);
			/*
			 * Check the response from the BigQuery interface to check if all the data has been appended to the
			 * table. If the response returns errors in the getInsertErrors() methods (each key is associated to
			 * an row) than the size will indicate how many rows failed.  In other words, if it is empty than 
			 * N rows were inserted correct but if X rows failed than N-X rows passed.
			 */
			if(response.getInsertErrors().isEmpty()) {
				log.info(String.format("Successfully written all %s rows to Big Query"
						+ " [%s.%s.%s]", size, projectId, datasetId, tableName));			
				c.output(KV.of(c.element().getKey(), size));
			}
			else {
				int actuallyWritten = size - response.getInsertErrors().size();
				log.info("Written " + actuallyWritten + " of " + size + 
						" rows. The following rows were not written: "
						+ response.getInsertErrors().toString());
				c.output(KV.of(c.element().getKey(), actuallyWritten));
			}
		}
		catch (Exception e) {
			log.error("Failed to push into Big Query", e);
		}
	}
}

