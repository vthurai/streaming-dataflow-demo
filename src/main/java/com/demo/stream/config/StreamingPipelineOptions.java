package com.demo.stream.config;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

public interface StreamingPipelineOptions extends DataflowPipelineOptions{
	
	public String getPublishArticlesSubscription();
	public void setPublishArticlesSubscription(String publishArticlesSubscription);
	
	public String getFailedArticlesSubscription();
	public void setFailedArticlesSubscription(String failedArticlesSubscription);
	
	public long getPublishArticleWindow();
	public void setPublishArticleWindow(long publishArticleWindow);

	public String getDatasetId();
	public void setDatasetId(String datasetId);	
	
	public String getArticleTableId();
	public void setArticleTableId(String articleTableId);	

	public String getErrorTableId();
	public void setErrorTableId(String errorTableId);
	
	
}