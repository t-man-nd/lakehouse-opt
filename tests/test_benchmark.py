"""D2 contracts: safe outputs, honest aggregates, and real nonempty queries."""

from copy import deepcopy
from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import Row

from src.delta_runtime import get_spark

from optimization_benchmark import (
    CONDITION_ORDER,
    canonical_result,
    define_queries,
    result_digest,
    summarize_measurements,
    validate_options,
)


@pytest.fixture
def source_dir(tmp_path):
    source = tmp_path / 'silver'
    (source / '_delta_log').mkdir(parents=True)
    return source


@pytest.mark.parametrize('iterations', [-1, 0, 1, 2])
def test_minimum_three_iterations(source_dir, tmp_path, iterations):
    with pytest.raises(ValueError, match='at least 3'):
        validate_options(source_dir, tmp_path / 'out', iterations, 32768)
    assert not (tmp_path / 'out').exists()


@pytest.mark.parametrize('iterations', [True, False, 3.5, '3', None, float('inf')])
def test_iterations_must_be_an_integer(source_dir, tmp_path, iterations):
    with pytest.raises(ValueError):
        validate_options(source_dir, tmp_path / 'out', iterations, 32768)


def test_layout_size_and_missing_source_guards(source_dir, tmp_path):
    with pytest.raises(ValueError, match='at least 4096'):
        validate_options(source_dir, tmp_path / 'out', 3, 4095)
    with pytest.raises(FileNotFoundError, match='Silver Delta table not found'):
        validate_options(tmp_path / 'absent', tmp_path / 'out', 3, 32768)


@pytest.mark.parametrize('relationship', ['same', 'child', 'parent', 'symlink'])
def test_source_and_output_must_not_overlap(source_dir, tmp_path, relationship):
    output = {'same': source_dir, 'child': source_dir / 'bench',
              'parent': tmp_path, 'symlink': tmp_path / 'linked-silver'}[relationship]
    if relationship == 'symlink':
        try:
            output.symlink_to(source_dir, target_is_directory=True)
        except OSError:
            if sys.platform == 'win32':
                import subprocess
                subprocess.run(
                    ['cmd', '/c', 'mklink', '/J', str(output), str(source_dir)],
                    check=True, capture_output=True, text=True,
                )
            else:
                raise
    marker = source_dir / 'keep.txt'
    marker.write_text('original Silver')
    with pytest.raises(ValueError, match='must not overlap'):
        validate_options(source_dir, output, 3, 32768)
    assert marker.read_text() == 'original Silver'


@pytest.mark.parametrize('existing_kind', ['directory', 'file'])
def test_fresh_output_guard_preserves_existing_data(source_dir, tmp_path, existing_kind):
    output = tmp_path / 'output'
    if existing_kind == 'directory':
        output.mkdir()
        marker = output / 'keep.txt'
    else:
        marker = output
    marker.write_text('do not replace')
    with pytest.raises(FileExistsError, match='fresh benchmark output'):
        validate_options(source_dir, output, 3, 32768)
    assert marker.read_text() == 'do not replace'


def test_valid_options_are_read_only(source_dir, tmp_path):
    output = tmp_path / 'fresh'
    assert validate_options(source_dir, output, 3, 4096) == (source_dir.resolve(), output.resolve())
    assert not output.exists()


def measured_samples():
    times = {'baseline': [100, 900, 110], 'zorder_1col': [165, 200, 170],
             'zorder_2col': [88, 77, 99], 'liquid_full': [111, 110, 109]}
    return [
        {'query_id': 'Q', 'condition_id': cid, 'execution_time_ms': elapsed,
         'files_scanned': 4 - index, 'bytes_read': 1050 + 75 * index}
        for cid, elapsed_times in times.items()
        for index, elapsed in enumerate(elapsed_times)
    ]


def test_medians_negative_improvement_and_actual_counter_samples():
    samples = measured_samples()
    original = deepcopy(samples)
    result = summarize_measurements(samples, [{'id': 'Q', 'matching_trip_count': 12}], CONDITION_ORDER)['Q']
    assert result['baseline']['median_ms'] == 110
    assert result['baseline']['times_ms'] == [100, 900, 110]
    assert result['baseline']['pct_improvement'] == 0
    assert result['zorder_1col']['median_ms'] == 170
    assert result['zorder_1col']['pct_improvement'] == -54.55
    assert result['zorder_2col']['pct_improvement'] == 20
    assert result['liquid_full']['pct_improvement'] == 0
    assert result['baseline']['runs'] == 3
    assert result['baseline']['files_scanned'] == [4, 3, 2]
    assert result['baseline']['bytes_read'] == [1050, 1125, 1200]
    assert result['baseline']['median_files_scanned'] == 3
    assert result['baseline']['median_bytes_read'] == 1125
    assert result['baseline']['matching_trip_count'] == 12
    assert samples == original


@pytest.mark.parametrize('missing_condition', ['baseline', 'liquid_full'])
def test_aggregation_rejects_missing_repetitions(missing_condition):
    samples = measured_samples()
    samples.remove(next(row for row in samples if row['condition_id'] == missing_condition))
    with pytest.raises(ValueError, match='Missing .*repetitions'):
        summarize_measurements(samples, [{'id': 'Q', 'matching_trip_count': 12}], CONDITION_ORDER)


def test_decimal_equality_is_scale_independent_and_row_order_independent():
    left = canonical_result([Row(zone=2, amount=Decimal('1.2300')),
                             Row(zone=1, amount=Decimal('0.0000'))])
    right = canonical_result([Row(zone=1, amount=Decimal('-0.0')),
                              Row(zone=2, amount=Decimal('1.23'))])
    assert left == right
    assert result_digest(left) == result_digest(right)


def test_decimal_equality_does_not_round_38_digit_spark_aggregates():
    first = canonical_result([Row(amount=Decimal('1234567890123456789012345678901234.0001'))])
    same = canonical_result([Row(amount=Decimal('1234567890123456789012345678901234.00010'))])
    different = canonical_result([Row(amount=Decimal('1234567890123456789012345678901234.0002'))])
    assert first == same
    assert first != different
    assert result_digest(first) != result_digest(different)


@pytest.fixture(scope='module')
def spark_queries():
    # PySpark reuses the gateway JVM; Delta jars must exist at its first launch.
    spark = get_spark('local[2]', 'D2QuerySelectionTests')
    spark.sparkContext.setLogLevel('ERROR')
    yield spark
    spark.stop()


def trip_frame(spark, trips):
    return spark.createDataFrame(
        [(pu, do, datetime.fromisoformat(ts), 10.0, 1.0) for pu, do, ts in trips],
        'PULocationID INT, DOLocationID INT, tpep_pickup_datetime TIMESTAMP, fare_amount DOUBLE, tip_amount DOUBLE',
    )


def test_queries_choose_deterministic_populated_same_day(spark_queries):
    frame = trip_frame(spark_queries, [
        (161, 7, '2025-01-05 00:00:00'), (161, 8, '2025-01-05 12:00:00'),
        (161, 7, '2025-01-05 23:59:59'), (161, 8, '2025-01-06 00:00:00'),
        (161, 7, '2025-01-06 12:00:00'), (161, 8, '2025-01-06 23:59:59'),
        *[(10, 20, '2025-02-01 12:00:00')] * 10,
    ])
    queries = define_queries(frame)
    assert define_queries(frame.repartition(2)) == queries
    assert queries[0]['predicate'] == 'PULocationID = 161'
    assert queries[0]['matching_trip_count'] == 6
    assert queries[1]['matching_trip_count'] == 3
    assert ">= TIMESTAMP '2025-01-05 00:00:00'" in queries[1]['predicate']
    assert "< TIMESTAMP '2025-01-06 00:00:00'" in queries[1]['predicate']
    assert {query['dimension'] for query in queries} == {'1D', '2D'}
    for query in queries:
        assert frame.where(query['predicate']).count() == query['matching_trip_count'] > 0


def test_query_fallback_ties_choose_smallest_zone_and_earliest_day(spark_queries):
    frame = trip_frame(spark_queries, [
        (8, 2, '2025-01-01 12:00:00'), (8, 2, '2025-01-02 12:00:00'),
        (7, 2, '2025-01-02 12:00:00'), (7, 2, '2025-01-01 12:00:00'),
    ])
    queries = define_queries(frame)
    assert queries[0]['predicate'] == 'PULocationID = 7'
    assert queries[1]['matching_trip_count'] == 1
    assert ">= TIMESTAMP '2025-01-01 00:00:00'" in queries[1]['predicate']
    with pytest.raises(ValueError, match='empty Silver table'):
        define_queries(frame.limit(0))


@pytest.mark.parametrize('pickup,dropoff', [(0, 0), (-20, -30)])
def test_query_ranges_include_zero_and_negative_nonnull_zones(spark_queries, pickup, dropoff):
    # B3 rejects missing locations; zero/negative non-null values are retained.
    frame = trip_frame(spark_queries, [
        (pickup, dropoff, '2025-01-01 12:00:00'),
        (pickup, dropoff, '2025-01-01 13:00:00'),
    ])
    queries = define_queries(frame)
    assert len(queries) == 3
    for query in queries:
        assert query['matching_trip_count'] == 2
        assert frame.where(query['predicate']).count() == 2
