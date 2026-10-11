# window 1 w/ log capture
mkdir -p logs
docker exec -it spark-master bash -lc '
export ENTITIES_PATH=/project/config/entities.yml
export PYTHONPATH=/project
cd /project &&
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --conf spark.jars.ivy=/tmp/ivy \
  --py-files /project/streaming.zip \
  /project/streaming/stream_job.py
' |& tee logs/slow_stream.log

# Window 2
timeout 180 python3 ingestion/producer.py --source file --file data/kaggle_tweets/samples/sent140_100k.jsonl   --interval 1.0

# Window 3
docker exec -it spark-master bash -lc "find /data/raw_parquet -type f | wc -l"
docker exec -it kafka kafka-run-class kafka.tools.GetOffsetShell   --bootstrap-server kafka:9092   --topic tweets   --time -1
docker stats --no-stream

docker exec spark-master bash -lc '
ls -lh /data/checkpoints/edges | tail -n 5
'

docker exec spark-master bash -lc '
ls -lh /data/raw_parquet | wc -l
du -sh /data/raw_parquet
'



# IMMEDIATELY AFTER
    # quick check after running
docker exec -it spark-master bash -lc 'tail -n 5 /data/metrics/stream_progress.jsonl'
    # grep the log
grep -E "batchId=|processedRowsPerSecond|falling behind" logs/slow_stream.log | tail -n 80
    # parquet summary
du -sh /data/raw_parquet
ls /data/raw_parquet | wc -l



# IN BETWEEN RUNS
    # Wipe Spark state/output
docker exec spark-master bash -lc 'rm -rf /data/checkpoints/with_figs_counts /data/checkpoints/edges /data/raw_parquet/*'
docker exec spark-master bash -lc 'mkdir -p /data/checkpoints/with_figs_counts /data/checkpoints/edges && chmod -R 777 /data/checkpoints'

    # Reset Kafka topic
docker exec kafka bash -lc '
kafka-topics --bootstrap-server kafka:29092 --delete --topic tweets || true
sleep 2
kafka-topics --bootstrap-server kafka:29092 --create --topic tweets --partitions 1 --replication-factor 1 || true
kafka-topics --bootstrap-server kafka:29092 --describe --topic tweets
'
