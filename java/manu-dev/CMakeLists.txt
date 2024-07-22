cmake_minimum_required(VERSION 3.4)

if(${CMAKE_VERSION} VERSION_LESS "3.11.4")
    message("Please consider switching to CMake 3.11.4 or newer")
endif()

project(manu-rocksjni VERSION 1.0)

add_definitions(-DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX)

set(JNI_NATIVE_SOURCES
    ${PROJECT_SOURCE_DIR}/../rocksjni/abstract_associative_merge_operator.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/abstract_compaction_filter.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/backup_engine_options.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/backupenginejni.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/cache.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/cassandra_compactionfilterjni.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/cassandra_value_operator.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/checkpoint.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/clock_cache.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/columnfamilyhandle.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/compaction_filter_factory.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/compaction_filter_factory_jnicallback.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/compaction_job_info.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/compaction_job_stats.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/compaction_options.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/compaction_options_fifo.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/compaction_options_universal.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/compact_range_options.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/comparator.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/comparatorjnicallback.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/compression_options.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/concurrent_task_limiter.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/config_options.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/env.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/env_options.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/event_listener.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/event_listener_jnicallback.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/export_import_files_metadatajni.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/filter.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/hyper_clock_cache.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/import_column_family_options.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/ingest_external_file_options.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/iterator.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/jni_perf_context.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/jnicallback.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/loggerjnicallback.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/lru_cache.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/memory_util.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/memtablejni.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/merge_operator.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/native_comparator_wrapper_test.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/optimistic_transaction_db.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/optimistic_transaction_options.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/options.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/options_util.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/persistent_cache.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/portal.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/ratelimiterjni.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/remove_emptyvalue_compactionfilterjni.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/restorejni.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/rocks_callback_object.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/rocksdb_exception_test.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/rocksjni.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/slice.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/snapshot.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/sst_file_manager.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/sst_file_writerjni.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/sst_file_readerjni.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/sst_file_reader_iterator.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/sst_partitioner.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/statistics.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/statisticsjni.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/stderr_logger.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/table.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/table_filter.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/table_filter_jnicallback.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/table_properties_collector_factory.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/testable_event_listener.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/thread_status.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/trace_writer.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/trace_writer_jnicallback.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/transaction.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/transaction_db.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/transaction_db_options.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/transaction_log.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/transaction_notifier.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/transaction_notifier_jnicallback.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/transaction_options.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/ttl.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/wal_filter.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/wal_filter_jnicallback.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/write_batch.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/writebatchhandlerjnicallback.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/write_batch_test.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/write_batch_with_index.cc
    ${PROJECT_SOURCE_DIR}/../rocksjni/write_buffer_manager.cc
)

include_directories(/usr/lib/jvm/java-11-openjdk-amd64/include)
include_directories(/usr/lib/jvm/java-11-openjdk-amd64/include/linux)
include_directories(${PROJECT_SOURCE_DIR}/..)
include_directories(${PROJECT_SOURCE_DIR}/../../include)
include_directories(${PROJECT_SOURCE_DIR}/../..)
include_directories(${PROJECT_SOURCE_DIR}/../../third-party/gtest-1.8.1/fused-src)

set(ROCKSDBJNI_MANU_LIB rocksdbjni-manu)

add_library(${ROCKSDBJNI_MANU_LIB} ${JNI_NATIVE_SOURCES})
