package com.demo.stream;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import com.demo.stream.config.PipelineConfig;
import com.demo.stream.config.StreamingPipelineOptions;

public class Application {
	public static void main(String[] args) {
		
		StreamingPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).as(StreamingPipelineOptions.class);
		new PipelineConfig().instantiatePipelineOptions(pipelineOptions);
		Pipeline p = Pipeline.create(pipelineOptions);

		StreamingPipeline.build(p).run();
	}

	
}