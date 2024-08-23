import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub, WriteToPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.transforms.window import FixedWindows
import json

# Project and table details
project_id = 'vadimzaripov-477-2022062208552'
table_id = 'experiments.store_records_timeseries_dataflow'
aggregated_table_id = 'experiments.store_records_timeseries_dataflow_aggregated'

# Pub/Sub topic and dead-letter topic
topic = 'projects/vadimzaripov-477-2022062208552/topics/timeseries-pos'
dead_letter_topic = 'projects/vadimzaripov-477-2022062208552/topics/timeseries-pos-dead-letter'

def parse_pubsub_message(message):
  try:
      data = json.loads(message.decode('utf-8'))
      data['date'] = data['timestamp'][:10]  # Extract date
      return data
  except Exception as e:
      # Handle parsing errors, log the message, and send to dead-letter queue
      print(f"Error parsing message: {message}, Error: {e}")
      return beam.pvalue.TaggedOutput('error', message)

# BigQuery table schemas
table_schema = {
  'fields': [
      {'name': 'store_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
      {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
      {'name': 'pos_number', 'type': 'INTEGER', 'mode': 'REQUIRED'},
      {'name': 'product_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
      {'name': 'value', 'type': 'FLOAT', 'mode': 'REQUIRED'},
      {'name': 'date', 'type': 'DATE', 'mode': 'REQUIRED'}
  ]
}

aggregated_table_schema = {
  'fields': [
      {'name': 'store_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
      {'name': 'product_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
      {'name': 'date', 'type': 'DATE', 'mode': 'REQUIRED'},
      {'name': 'total_value', 'type': 'FLOAT', 'mode': 'REQUIRED'}
  ]
}

def aggregate_by_date(elements):
  return sum(el for el in elements)  # Directly sum the elements in the iterable

# Pipeline options
options = PipelineOptions(
  streaming=True,  # For continuous processing
  project=project_id
)

# Pipeline definition
with beam.Pipeline(options=options) as p:
  parsed_messages = (
      p 
      | 'Read from PubSub' >> ReadFromPubSub(topic=topic)
      | 'Parse JSON' >> beam.Map(parse_pubsub_message).with_outputs('error', main='parsed')
  )

  # Write successful messages to BigQuery
  (
      parsed_messages['parsed'] 
      | 'Window into Batches' >> beam.WindowInto(FixedWindows(60))  # 1-minute windows
      | 'Write to BigQuery' >> WriteToBigQuery(
          table=table_id,
          schema=table_schema,
          create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
          write_disposition=BigQueryDisposition.WRITE_APPEND
      )
  )

  # Write failed messages to dead-letter topic
  (
      parsed_messages['error'] 
      | 'Write to Dead-Letter Topic' >> WriteToPubSub(topic=dead_letter_topic)
  )

  # Aggregate and write to another BigQuery table
  (
      parsed_messages['parsed'] 
      | 'Window into Batches for Aggregation' >> beam.WindowInto(FixedWindows(60))  # Apply windowing before GroupByKey
      | 'Extract Store, Product, Date, and Value' >> beam.Map(lambda x: ((x['store_id'], x['product_id'], x['date']), x['value']))
      #| 'Group by Store, Product, and Date' >> beam.GroupByKey()
      | 'Aggregate by Store, Product, and Date' >> beam.CombinePerKey(sum)
      | 'Format for BigQuery' >> beam.Map(lambda x: {'store_id': x[0][0], 'product_id': x[0][1], 'date': x[0][2], 'total_value': x[1]})
      | 'Write Aggregated to BigQuery' >> WriteToBigQuery(
          table=aggregated_table_id,
          schema=aggregated_table_schema,
          create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
          write_disposition=BigQueryDisposition.WRITE_APPEND
      )
  )