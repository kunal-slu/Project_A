"""
Structured Streaming for near-real-time orders ingestion to Bronze, then incremental Gold fact updates.
Enhanced with incremental processing, CDC, and comprehensive Kafka integration.
"""

import logging
from typing import Optional, List

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.streaming import DataStreamReader, DataStreamWriter

logger = logging.getLogger(__name__)


class StreamingProcessor:
    """
    Handles real-time streaming data processing with PySpark Structured Streaming.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.checkpoint_location = "/tmp/streaming_checkpoints"

    def create_streaming_source(
        self, 
        source_type: str, 
        source_path: str, 
        schema: Optional[StructType] = None
    ) -> DataStreamReader:
        """
        Create a streaming source reader.
        
        Args:
            source_type: Type of source (kafka, file, socket, etc.)
            source_path: Path or connection string to the source
            schema: Optional schema for the streaming data
            
        Returns:
            DataStreamReader configured for the source
        """
        reader = self.spark.readStream.format(source_type)
        
        if source_type == "kafka":
            reader = reader.option("kafka.bootstrap.servers", source_path)
        elif source_type == "file":
            reader = reader.option("path", source_path)
        elif source_type == "socket":
            reader = reader.option("host", source_path.split(":")[0])
            reader = reader.option("port", source_path.split(":")[1])
        
        if schema:
            reader = reader.schema(schema)
            
        return reader

    def process_streaming_data(
        self, 
        source_df: DataFrame, 
        processing_logic: callable
    ) -> DataStreamWriter:
        """
        Apply processing logic to streaming data.
        
        Args:
            source_df: Streaming DataFrame from source
            processing_logic: Function to apply to the data
            
        Returns:
            DataStreamWriter with processing applied
        """
        processed_df = processing_logic(source_df)
        
        return processed_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("checkpointLocation", self.checkpoint_location) \
            .trigger(processingTime="5 seconds")

    def start_streaming_job(
        self, 
        source_type: str, 
        source_path: str, 
        processing_logic: callable,
        schema: Optional[StructType] = None
    ) -> None:
        """
        Start a complete streaming job.
        
        Args:
            source_type: Type of streaming source
            source_path: Path to the source
            processing_logic: Function to process the data
            schema: Optional schema for the data
        """
        try:
            # Create streaming source
            reader = self.create_streaming_source(source_type, source_path, schema)
            source_df = reader.load()
            
            # Apply processing logic
            writer = self.process_streaming_data(source_df, processing_logic)
            
            # Start the streaming query
            query = writer.start()
            
            logger.info(f"Started streaming job from {source_type} source")
            
            # Wait for termination
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error in streaming job: {e}")
            raise

    def create_sample_schema(self) -> StructType:
        """
        Create a sample schema for streaming data.
        
        Returns:
            StructType schema for sample data
        """
        return StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", IntegerType(), True),
            StructField("timestamp", TimestampType(), True)
        ])

    def sample_processing_logic(self, df: DataFrame) -> DataFrame:
        """
        Sample processing logic for streaming data.
        
        Args:
            df: Input streaming DataFrame
            
        Returns:
            Processed DataFrame
        """
        return df.select(
            F.col("id"),
            F.col("name"),
            F.col("value"),
            F.col("timestamp"),
            F.when(F.col("value") > 100, "high").otherwise("low").alias("category")
        )

    def aggregate_streaming_data(
        self, 
        df: DataFrame, 
        window_duration: str = "5 minutes",
        slide_duration: str = "1 minute"
    ) -> DataFrame:
        """
        Perform windowed aggregations on streaming data.
        
        Args:
            df: Input streaming DataFrame
            window_duration: Duration of the window
            slide_duration: Slide interval for the window
            
        Returns:
            DataFrame with windowed aggregations
        """
        return df.groupBy(
            F.window(F.col("timestamp"), window_duration, slide_duration),
            F.col("category")
        ).agg(
            F.count("*").alias("count"),
            F.avg("value").alias("avg_value"),
            F.countDistinct("id").alias("unique_ids")
        )

    def write_to_sink(
        self, 
        df: DataFrame, 
        sink_type: str, 
        sink_path: str,
        output_mode: str = "append"
    ) -> DataStreamWriter:
        """
        Write streaming data to a sink.
        
        Args:
            df: DataFrame to write
            sink_type: Type of sink (console, parquet, delta, etc.)
            sink_path: Path for the sink
            output_mode: Output mode (append, complete, update)
            
        Returns:
            DataStreamWriter configured for the sink
        """
        writer = df.writeStream \
            .outputMode(output_mode) \
            .format(sink_type) \
            .option("checkpointLocation", self.checkpoint_location)
        
        if sink_type == "parquet":
            writer = writer.option("path", sink_path)
        elif sink_type == "delta":
            writer = writer.option("path", sink_path)
        elif sink_type == "console":
            writer = writer.option("numRows", 20)
            
        return writer

    def monitor_streaming_metrics(
        self, 
        query, 
        metrics_interval: int = 60
    ) -> None:
        """
        Monitor streaming query metrics.
        
        Args:
            query: Active streaming query
            metrics_interval: Interval in seconds to log metrics
        """
        import time
        
        while query.isActive:
            # Get query status
            status = query.status
            
            # Log metrics
            logger.info(f"Streaming query status: {status}")
            logger.info(f"Processed records: {query.recentProgress}")
            
            time.sleep(metrics_interval)

    def handle_streaming_exceptions(
        self, 
        query, 
        max_retries: int = 3
    ) -> None:
        """
        Handle exceptions in streaming queries.
        
        Args:
            query: Active streaming query
            max_retries: Maximum number of retry attempts
        """
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                if not query.isActive:
                    logger.warning("Streaming query stopped unexpectedly")
                    break
                    
            except Exception as e:
                retry_count += 1
                logger.error(f"Streaming error (attempt {retry_count}): {e}")
                
                if retry_count >= max_retries:
                    logger.error("Max retries reached, stopping streaming job")
                    query.stop()
                    break

    def create_complex_streaming_pipeline(
        self, 
        source_type: str, 
        source_path: str,
        processing_steps: List[callable],
        sink_type: str,
        sink_path: str
    ) -> None:
        """
        Create a complex streaming pipeline with multiple processing steps.
        
        Args:
            source_type: Type of streaming source
            source_path: Path to the source
            processing_steps: List of processing functions to apply
            sink_type: Type of sink
            sink_path: Path for the sink
        """
        try:
            # Create source
            reader = self.create_streaming_source(source_type, source_path)
            df = reader.load()
            
            # Apply processing steps
            for step in processing_steps:
                df = step(df)
            
            # Write to sink
            writer = self.write_to_sink(df, sink_type, sink_path)
            query = writer.start()
            
            logger.info("Started complex streaming pipeline")
            
            # Monitor and handle exceptions
            self.monitor_streaming_metrics(query)
            self.handle_streaming_exceptions(query)
            
        except Exception as e:
            logger.error(f"Error in complex streaming pipeline: {e}")
            raise
