"""D2: reproducible Silver layout experiment with observed Spark scan metrics.

The required sequence is baseline, ZORDER(1), ZORDER(2), liquid clustering FULL.
A separate paired compaction control retains the original assignment's OPTIMIZE
comparison. All layouts contain the exact same pinned Silver rows. The small
classroom sample uses a disclosed small file target, not a production scale claim.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
from importlib.metadata import version as package_version
import json
from pathlib import Path
import platform
import shutil
import statistics
import time
from uuid import uuid4

from delta import DeltaTable
from pyspark.sql import functions as F

from src.delta_runtime import get_spark, latest_version, require_same_rows, snapshot, write_json
from src.benchmark_metrics import collect_query_metrics

CONDITION_ORDER = ('baseline', 'zorder_1col', 'zorder_2col', 'liquid_full')
LAYOUTS = {
    'baseline': ('Baseline', []),
    'zorder_1col': ('Z-ORDER 1 column', ['PULocationID']),
    'zorder_2col': ('Z-ORDER 2 columns', ['PULocationID', 'tpep_pickup_datetime']),
    'liquid_full': ('CLUSTER BY + OPTIMIZE FULL', ['PULocationID', 'tpep_pickup_datetime']),
    'compacted': ('OPTIMIZE compaction control', []),
}


@contextmanager
def spark_settings(spark, settings):
    """Restore settings even if preparation or a measurement raises."""
    previous = {key: spark.conf.get(key, None) for key in settings}
    try:
        for key, value in settings.items():
            spark.conf.set(key, str(value))
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)


def validate_options(silver_dir, output_dir, iterations, target_file_size):
    if not isinstance(iterations, int) or isinstance(iterations, bool) or iterations < 3:
        raise ValueError('D2 requires at least 3 timed iterations per condition')
    if not isinstance(target_file_size, int) or isinstance(target_file_size, bool) or target_file_size < 4096:
        raise ValueError('target_file_size must be at least 4096 bytes')
    source, output = Path(silver_dir).resolve(), Path(output_dir).resolve()
    if '`' in str(source) or '`' in str(output):
        raise ValueError('Benchmark paths must not contain SQL identifier delimiters')
    if source == output or source in output.parents or output in source.parents:
        raise ValueError('Benchmark output and source directories must not overlap')
    if not (source / '_delta_log').is_dir():
        raise FileNotFoundError(f'Silver Delta table not found: {source}')
    if output.exists():
        raise FileExistsError(f'Use a fresh benchmark output directory: {output}')
    return source, output


def table_detail(spark, path):
    row = DeltaTable.forPath(spark, str(path)).detail().first().asDict(recursive=True)
    return {'version': latest_version(spark, str(path)), 'num_files': int(row['numFiles']),
            'size_bytes': int(row['sizeInBytes']), 'format': row['format'],
            'clustering_columns': row.get('clusteringColumns', []),
            'partition_columns': row['partitionColumns']}


def prepare_layouts(spark, source, output, target_file_size, baseline_files=32):
    """Physically copy one fresh local Delta baseline, then change only its layout."""
    base = output / 'tables' / 'baseline'
    source.repartition(baseline_files).write.format('delta').mode('errorifexists').save(str(base))
    conditions = {}
    baseline_detail = table_detail(spark, base)
    for key, (name, columns) in LAYOUTS.items():
        path = output / 'tables' / key
        if key != 'baseline':
            shutil.copytree(base, path)
        before = table_detail(spark, path)
        started = time.perf_counter()
        if key.startswith('zorder'):
            DeltaTable.forPath(spark, str(path)).optimize().executeZOrderBy(*columns).collect()
        elif key == 'liquid_full':
            spark.sql(f'ALTER TABLE delta.`{path}` CLUSTER BY (PULocationID, tpep_pickup_datetime)').collect()
            spark.sql(f'OPTIMIZE delta.`{path}` FULL').collect()
        elif key == 'compacted':
            # Original assignment target: ~1 GiB; a small sample makes one smaller file.
            with spark_settings(spark, {'spark.databricks.delta.optimize.maxFileSize': 1073741824,
                                        'spark.databricks.delta.optimize.minFileSize': 1073741824}):
                spark.sql(f'OPTIMIZE delta.`{path}`').collect()
        optimization_seconds = round(time.perf_counter() - started, 6)
        after = table_detail(spark, path)
        require_same_rows(source, snapshot(spark, str(path)), f'{key} preserves pinned Silver')
        history = [r.asDict(recursive=True) for r in DeltaTable.forPath(spark, str(path)).history().collect()]
        if before['num_files'] != baseline_detail['num_files'] or before['size_bytes'] != baseline_detail['size_bytes']:
            raise AssertionError('All layouts must start from identical physical baseline files')
        if key == 'liquid_full' and after['clustering_columns'] != columns:
            raise AssertionError('Liquid clustering columns were not enabled')
        conditions[key] = {'id': key, 'name': name, 'path': str(path), 'columns': columns,
                           'before': before, 'after': after, 'same_rows_as_source': True,
                           'optimization_seconds': optimization_seconds,
                           'target_file_size_bytes': 1073741824 if key == 'compacted' else target_file_size,
                           'history': history}
        print(f"[D2 layout] {key}: {before['num_files']} -> {after['num_files']} files", flush=True)
    return conditions


def define_queries(source):
    """Choose deterministic, nonempty predicates from the pinned snapshot.

    Selection uses counts/timestamps only, before any timing. It never chooses
    queries by observed speedup. Bounds are stored verbatim in the result JSON.
    """
    locations = source.groupBy('PULocationID').count().orderBy(F.desc('count'), 'PULocationID').collect()
    if not locations:
        raise ValueError('Cannot benchmark an empty Silver table')
    pu = 161 if any(r['PULocationID'] == 161 for r in locations) else int(locations[0]['PULocationID'])
    selected = source.filter(F.col('PULocationID') == pu)
    day = selected.select(F.to_date('tpep_pickup_datetime').alias('day')).groupBy('day').count().orderBy(F.desc('count'), 'day').first()['day']
    start = datetime.combine(day, datetime.min.time())
    end = start + timedelta(days=1)
    ts_start, ts_end = (v.strftime('%Y-%m-%d %H:%M:%S') for v in (start, end))
    do = int(selected.groupBy('DOLocationID').count().orderBy(F.desc('count'), 'DOLocationID').first()['DOLocationID'])
    projection = ('COUNT(*) AS trip_count, AVG(CAST(fare_amount AS DECIMAL(18,4))) AS avg_fare, '
                  'SUM(CAST(tip_amount AS DECIMAL(18,4))) AS total_tip')
    predicates = [
        ('Q1_1D_PointLookup', '1D', f'PULocationID = {pu}'),
        ('Q2_2D_LocationAndTime', '2D', f"PULocationID = {pu} AND tpep_pickup_datetime >= TIMESTAMP '{ts_start}' AND tpep_pickup_datetime < TIMESTAMP '{ts_end}'"),
        ('Q3_2D_RangeAggregation', '2D', f'PULocationID BETWEEN {pu-15} AND {pu+15} AND DOLocationID BETWEEN {do-15} AND {do+15}'),
    ]
    queries = []
    for query_id, dimension, predicate in predicates:
        expected = source.where(predicate).agg(F.count('*').alias('count')).first()['count']
        if expected <= 0:
            raise AssertionError(f'{query_id} must match real trips')
        queries.append({'id': query_id, 'dimension': dimension, 'predicate': predicate,
                        'sql': f'SELECT {projection} FROM {{table}} WHERE {predicate}',
                        'matching_trip_count': int(expected)})
    return queries


def canonical_result(rows):
    def normalize(value):
        if isinstance(value, Decimal):
            # Decimal.normalize() obeys the ambient precision (usually 28),
            # which can silently round Spark DECIMAL(38, ...) aggregates.
            text = format(value, 'f')
            text = text.rstrip('0').rstrip('.') if '.' in text else text
            return '0' if value == 0 else text
        if isinstance(value, (datetime,)):
            return value.isoformat()
        return value
    records = [{key: normalize(value) for key, value in row.asDict().items()} for row in rows]
    return sorted(records, key=lambda row: json.dumps(row, sort_keys=True))


def result_digest(rows):
    return hashlib.sha256(json.dumps(rows, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def summarize_measurements(measurements, queries, order):
    """Aggregate observed samples; preserve every sample in the outer result."""
    result = {}
    for query in queries:
        qid = query['id']
        baseline = [m['execution_time_ms'] for m in measurements if m['query_id'] == qid and m['condition_id'] == 'baseline']
        if len(baseline) < 3:
            raise ValueError(f'Missing baseline repetitions: {qid}')
        baseline_median = statistics.median(baseline)
        result[qid] = {}
        for cid in order:
            samples = [m for m in measurements if m['query_id'] == qid and m['condition_id'] == cid]
            if len(samples) < 3:
                raise ValueError(f'Missing repetitions: {qid}/{cid}')
            times = [m['execution_time_ms'] for m in samples]
            median = statistics.median(times)
            result[qid][cid] = {
                'runs': len(samples), 'times_ms': times, 'median_ms': round(median, 4),
                'min_ms': min(times), 'max_ms': max(times),
                'stdev_ms': round(statistics.stdev(times), 4),
                'pct_improvement': round(100 * (baseline_median - median) / baseline_median, 2) if baseline_median else None,
                'files_scanned': [m['files_scanned'] for m in samples],
                'bytes_read': [m['bytes_read'] for m in samples],
                'median_files_scanned': statistics.median(m['files_scanned'] for m in samples),
                'median_bytes_read': statistics.median(m['bytes_read'] for m in samples),
                'matching_trip_count': query['matching_trip_count'],
            }
    return result


def run_suite(spark, conditions, queries, iterations, order, expected_results, phase):
    measurements, warmups = [], []
    # Warm every query/layout once before timed comparisons. Cache policy is warm OS cache.
    for query in queries:
        for cid in order:
            spark.catalog.clearCache()
            frame = spark.sql(query['sql'].format(table=f"delta.`{conditions[cid]['path']}`"))
            rows = frame.collect()
            canonical = canonical_result(rows)
            if canonical != expected_results[query['id']]:
                raise AssertionError(f'Warmup result mismatch: {query["id"]}/{cid}')
            warmups.append({'query_id': query['id'], 'condition_id': cid,
                            'result_sha256': result_digest(canonical)})
    for query in queries:
        for iteration in range(1, iterations + 1):
            for cid in order:
                spark.catalog.clearCache()
                frame = spark.sql(query['sql'].format(table=f"delta.`{conditions[cid]['path']}`"))
                rows, metrics = collect_query_metrics(spark, frame, f'd2-{phase}-{uuid4().hex}')
                canonical = canonical_result(rows)
                if canonical != expected_results[query['id']]:
                    raise AssertionError(f'Timed result mismatch: {query["id"]}/{cid}')
                if not 0 <= metrics['files_scanned'] <= conditions[cid]['after']['num_files']:
                    raise AssertionError('Observed scan file count is outside the table file count')
                measurements.append({**metrics, 'phase': phase, 'iteration': iteration,
                                     'query_id': query['id'], 'condition_id': cid,
                                     'files_skipped': conditions[cid]['after']['num_files'] - metrics['files_scanned'],
                                     'result_rows': canonical, 'result_sha256': result_digest(canonical),
                                     'matching_trip_count': query['matching_trip_count']})
            print(f'[D2 {phase}] {query["id"]} round {iteration}/{iterations} complete', flush=True)
    return {'warmups': warmups, 'measurements': measurements,
            'results': summarize_measurements(measurements, queries, order)}


def generate_markdown_report(result):
    metadata = result['benchmark_metadata']
    lines = ['# D2 — Silver storage optimization benchmark', '',
             f"Run UTC: {result['timestamp']}. Source: `{result['source']['path']}`, version {result['source']['version']}, **{result['source']['rows']:,} rows**.", '',
             '## Method', '',
             'The four required conditions run in order baseline → Z-ORDER 1 column → Z-ORDER 2 columns → CLUSTER BY + OPTIMIZE FULL, repeated for each query. All start from byte-identical baseline files and preserve every pinned Silver row.', '',
             f"Each query/layout has one untimed warmup and {metadata['iterations']} timed executions. Spark DataFrame/catalog cache is cleared each time. OS page cache is **not** flushed: this is a warm-cache local benchmark. Delta planning is completed before timing; collection, task execution and result transfer are timed. Preparation, optimization and metrics collection are excluded.", '',
             '`files_scanned` is the executed Spark scan operator counter. `bytes_read` is the filesystem input-byte counter of successful tasks in that query’s jobs; it is not physical disk traffic and not a sum of whole Parquet file sizes. Job/stage IDs and raw samples are retained in results.json.', '',
             f"Runtime: Spark {metadata['spark_version']}, Delta {metadata['delta_version']}, {metadata['master']}, UTC; AQE disabled during measurements. Baseline partitions: {metadata.get('baseline_partitions', 32)}. Layout target: {metadata['target_file_size_bytes']:,} bytes. Supplemental compaction uses the original 1 GiB target; actual file sizes are reported below.", '',
             '## Layouts before/after', '',
             '| Condition | Files before | Files after | Bytes before | Bytes after | Optimize seconds |',
             '|---|---:|---:|---:|---:|---:|']
    for condition in result['conditions'].values():
        lines.append(f"| {condition['name']} | {condition['before']['num_files']} | {condition['after']['num_files']} | {condition['before']['size_bytes']} | {condition['after']['size_bytes']} | {condition['optimization_seconds']} |")
    control = result['compaction_control']['condition']
    lines.append(f"| {control['name']} | {control['before']['num_files']} | {control['after']['num_files']} | {control['before']['size_bytes']} | {control['after']['size_bytes']} | {control['optimization_seconds']} |")
    for query in result['queries']:
        lines += ['', f"## {query['id']} ({query['dimension']})", '',
                  f"Predicate: `{query['predicate']}`. Matching trips: **{query['matching_trip_count']}**. All results equal the pinned source query.", '',
                  '| Condition | Median ms | Improvement % | Median files scanned | Median input bytes |',
                  '|---|---:|---:|---:|---:|']
        for cid in CONDITION_ORDER:
            row = result['results'][query['id']][cid]
            lines.append(f"| {LAYOUTS[cid][0]} | {row['median_ms']} | {row['pct_improvement']} | {row['median_files_scanned']} | {row['median_bytes_read']} |")
    lines += ['', '## Supplemental compaction control', '',
              'This separate paired phase runs baseline → compacted, repeated three or more times after its own warmups. Its medians must be compared within this phase, not substituted into the four-condition phase.', '',
              '| Query | Baseline median ms | Compacted median ms | Improvement % |', '|---|---:|---:|---:|']
    for query in result['queries']:
        stats = result['compaction_control']['results'][query['id']]
        lines.append(f"| {query['id']} | {stats['baseline']['median_ms']} | {stats['compacted']['median_ms']} | {stats['compacted']['pct_improvement']} |")
    lines += ['', '## Observations and threats to validity', '',
              'Positive improvement means faster in this run; negative improvement means slower. File skipping and elapsed time are reported separately. No speedup is assumed or selected after observing timings.', '',
              f"- This input has {result['source']['rows']:,} rows. The file target is an experimental setting, not a production recommendation; Parquet/footer and Spark scheduling costs can dominate at small scales.",
              '- Query predicates are selected deterministically from available data before timing, not from observed speedups. Sampling provenance is recorded under source.sampling when supplied; absence of that metadata is not evidence of a representative sample. Controlled B1 defects and synthetic CDC/evolution rows must be distinguished from the source population.',
              '- One warmup and interleaving reduce some startup effects but do not eliminate JIT, GC, OS cache or fixed-order bias. Median does not eliminate all outliers. Raw runs and sample dispersion remain available.',
              '- The same machine performs reads and writes; these are local filesystem results. Task bytes include filesystem reads and can include read-ahead/footer activity, not a physical disk measurement.',
              '- Compaction, clustering, compression and file count interact. Report preparation cost and all file counts; do not attribute every elapsed-time change solely to data skipping.',
              f"- {metadata['iterations']} repetitions per condition report observed variability, not a claim of statistical significance. These local warm-cache results cannot establish production capacity.", '',
              '## References', '',
              '- [Delta liquid clustering and OPTIMIZE FULL](https://docs.delta.io/delta-clustering/)',
              '- [Delta OPTIMIZE and Z-order](https://docs.delta.io/optimizations-oss/)',
              '- [Spark monitoring and task input metrics](https://spark.apache.org/docs/4.0.1/monitoring.html)', '']
    return '\n'.join(lines)


def run_benchmark(spark, silver_dir, output_dir, *, iterations=3, source_version=None,
                  target_file_size=32768, baseline_files=32, sampling_metadata=None):
    if not isinstance(baseline_files, int) or isinstance(baseline_files, bool) or baseline_files < 2:
        raise ValueError('baseline_files must be an integer of at least 2')
    source_path, output = validate_options(silver_dir, output_dir, iterations, target_file_size)
    version = latest_version(spark, str(source_path)) if source_version is None else int(source_version)
    source = snapshot(spark, str(source_path), version)
    source_count = source.count()
    if not source_count:
        raise ValueError('Cannot benchmark an empty Silver table')
    output.mkdir(parents=True, exist_ok=False)
    settings = {'spark.sql.session.timeZone': 'UTC', 'spark.sql.adaptive.enabled': 'false',
                'spark.databricks.delta.optimize.maxFileSize': target_file_size,
                'spark.databricks.delta.optimize.minFileSize': target_file_size,
                'spark.databricks.delta.optimize.maxThreads': 2,
                'spark.databricks.delta.optimize.repartition.enabled': 'true'}
    with spark_settings(spark, settings):
        conditions = prepare_layouts(spark, source, output, target_file_size, baseline_files)
        queries = define_queries(source)
        view = f'd2_source_{uuid4().hex}'
        source.createOrReplaceTempView(view)
        try:
            expected = {q['id']: canonical_result(spark.sql(q['sql'].format(table=view)).collect()) for q in queries}
        finally:
            spark.catalog.dropTempView(view)
        main = run_suite(spark, conditions, queries, iterations, CONDITION_ORDER, expected, 'required')
        control = run_suite(spark, conditions, queries, iterations, ('baseline', 'compacted'), expected, 'compaction')
        result = {
            'status': 'passed', 'timestamp': datetime.now(timezone.utc).isoformat(),
            'source': {'path': str(source_path), 'version': version, 'rows': source_count,
                       'schema': source.schema.jsonValue(), 'sampling': sampling_metadata},
            'benchmark_metadata': {'spark_version': spark.version, 'delta_version': package_version('delta-spark'),
                                   'master': spark.sparkContext.master, 'platform': platform.platform(),
                                   'iterations': iterations, 'warmups_per_query_condition': 1,
                                   'condition_order': list(CONDITION_ORDER), 'target_file_size_bytes': target_file_size,
                                   'baseline_partitions': baseline_files,
                                   'cache_policy': 'warm OS cache; Spark catalog cleared; no persisted dataframes',
                                   'timing_scope': 'collect only after executed-plan preparation',
                                   'source_selection': 'pinned Silver snapshot; no synthetic replication'},
            'conditions': {key: conditions[key] for key in CONDITION_ORDER}, 'queries': queries,
            **main,
            'compaction_control': {'condition': conditions['compacted'], **control},
            'validation': {'all_layouts_preserve_source_rows': True, 'all_query_results_equal_source': True,
                           'all_queries_nonempty': True, 'required_timed_executions': len(main['measurements']),
                           'control_timed_executions': len(control['measurements'])},
            'artifacts': {'results': str(output / 'results.json'), 'report': str(output / 'report.md')},
        }
        write_json(output / 'results.json', result)
        (output / 'report.md').write_text(generate_markdown_report(result), encoding='utf-8')
    print(f'[D2 PASS] {output / "results.json"}', flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--silver-dir', default='data/silver/taxi_trips')
    parser.add_argument('--output-dir', default=None, help='Fresh directory for tables and evidence')
    parser.add_argument('--source-version', type=int)
    parser.add_argument('--iterations', type=int, default=3)
    parser.add_argument('--target-file-size', type=int, default=32768)
    parser.add_argument('--baseline-files', type=int, default=32)
    parser.add_argument('--master', default='local[2]')
    args = parser.parse_args()
    output = args.output_dir or str(Path('data/benchmark') / datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ'))
    validate_options(args.silver_dir, output, args.iterations, args.target_file_size)
    spark = get_spark(args.master, 'D2PerformanceLab')
    try:
        run_benchmark(spark, args.silver_dir, output, iterations=args.iterations,
                      source_version=args.source_version, target_file_size=args.target_file_size,
                      baseline_files=args.baseline_files)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
