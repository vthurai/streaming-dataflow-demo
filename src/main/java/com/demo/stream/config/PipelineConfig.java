package com.demo.stream.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;

public class PipelineConfig {
	
	private Properties properties;
	
	public PipelineConfig() {
		properties = new Properties();
		try(InputStream inputStream = new FileInputStream(new File("src/main/resources/streaming_pipeline.properties"))){
				properties.load(inputStream);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void instantiatePipelineOptions(StreamingPipelineOptions pipelineOptions) {
		
		pipelineOptions.setPublishArticlesSubscription(properties.getProperty("publishArticlesSubscription"));
		pipelineOptions.setFailedArticlesSubscription(properties.getProperty("failedArticlesSubscription"));
		pipelineOptions.setPublishArticleWindow(Long.parseLong(properties.getProperty("publishArticleWindow")));

		pipelineOptions.setProject(properties.getProperty("projectId"));
		pipelineOptions.setDatasetId(properties.getProperty("datasetId"));
		pipelineOptions.setArticleTableId(properties.getProperty("articleTableId"));
		pipelineOptions.setErrorTableId(properties.getProperty("errorTableId"));

		pipelineOptions.setRunner(DataflowRunner.class);
		pipelineOptions.setAutoscalingAlgorithm(DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.THROUGHPUT_BASED);
		pipelineOptions.setStreaming(true);
		pipelineOptions.setJobName(properties.getProperty("appName"));
		pipelineOptions.setZone(properties.getProperty("zone"));
		pipelineOptions.setStagingLocation(properties.getProperty("stagingLocation"));
		//pipelineOptions.setGcpTempLocation(properties.getProperty("tempLocation"));
		pipelineOptions.setNumWorkers(Integer.parseInt(properties.getProperty("numWorkers")));
		pipelineOptions.setMaxNumWorkers(Integer.parseInt(properties.getProperty("maxNumWorkers")));
		pipelineOptions.setWorkerMachineType(properties.getProperty("workerMachineType"));
		pipelineOptions.setServiceAccount(properties.getProperty("serviceAccount"));
	}
	
}
