#!/usr/bin/env python3
"""Архиватор Kafka → S3 с отдельной consumer group (пункт 2 REQUIREMENTS)."""
from __future__ import annotations

import argparse
import json
import time
import uuid
from collections import defaultdict

import boto3
from confluent_kafka import Consumer


def business_date(prefix: str, row: dict) -> str:
    key = "event_time" if prefix == "raw_events" else "effective_at"
    return row[key][:10]  # YYYY-MM-DD из ISO-строки


def flush_batch(s3, bucket: str, prefix: str, date: str, rows: list[dict]) -> str:
    key = f"{prefix}/dt={date}/part-{uuid.uuid4().hex}.jsonl"
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
    s3.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
    return key


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap-servers", default="localhost:9092")
    p.add_argument("--events-topic", default="cinema.events")
    p.add_argument("--profiles-topic", default="cinema.profile_changes")
    p.add_argument("--group-id", default="s3_archiver_group")
    p.add_argument("--s3-endpoint", default="http://localhost:9002")
    p.add_argument("--access-key", default="minioadmin")
    p.add_argument("--secret-key", default="minioadmin")
    p.add_argument("--bucket", default="dwh-archive")
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--idle-timeout", type=float, default=10.0)
    p.add_argument("--max-runtime", type=float, default=600.0)
    args = p.parse_args()

    s3 = boto3.client(
        "s3",
        endpoint_url=args.s3_endpoint,
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        region_name="us-east-1",
    )
    try:
        s3.create_bucket(Bucket=args.bucket)
    except Exception:
        pass

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": args.group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,
    })
    consumer.subscribe([args.events_topic, args.profiles_topic])

    buffers: dict[tuple[str, str], list[dict]] = defaultdict(list)
    last_message = time.monotonic()
    start = time.monotonic()
    total = 0
    saved_files = 0

    try:
        while True:
            if time.monotonic() - start > args.max_runtime:
                print(f"Достигнут --max-runtime ({args.max_runtime}s)")
                break
            msg = consumer.poll(1.0)
            if msg is None:
                if total > 0 and time.monotonic() - last_message > args.idle_timeout:
                    print(f"Нет новых сообщений {args.idle_timeout}s, остановка")
                    break
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue
            last_message = time.monotonic()
            total += 1
            try:
                row = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"Пропущено (parse error): {e}")
                continue

            prefix = "raw_events" if msg.topic() == args.events_topic else "raw_profile_changes"
            date = business_date(prefix, row)
            buffers[(prefix, date)].append(row)

            for key in list(buffers.keys()):
                if len(buffers[key]) >= args.batch_size:
                    p_, d_ = key
                    k = flush_batch(s3, args.bucket, p_, d_, buffers.pop(key))
                    saved_files += 1
                    print(f"  → s3://{args.bucket}/{k} ({args.batch_size} строк)")

        for (p_, d_), rows in buffers.items():
            k = flush_batch(s3, args.bucket, p_, d_, rows)
            saved_files += 1
            print(f"  → s3://{args.bucket}/{k} ({len(rows)} строк)")

        print(f"Итого: {total} сообщений, {saved_files} файлов в S3")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
