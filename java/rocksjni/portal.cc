/*
 * @author emmanuel.marchand@babbar.tech
 */

#include "rocksjni/portal.h"

thread_local std::unique_ptr<ROCKSDB_NAMESPACE::JniEnv> ROCKSDB_NAMESPACE::JniEnv::cache_ = nullptr;

volatile bool ROCKSDB_NAMESPACE::JniEnv::shutdown_ = false;
