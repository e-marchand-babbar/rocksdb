//
// Created by emmanuel.marchand on 12/1/23.
//

#include "rocksjni/portal.h"

thread_local std::unique_ptr<ROCKSDB_NAMESPACE::JniEnv> ROCKSDB_NAMESPACE::JniEnv::cache_ = nullptr;

volatile bool ROCKSDB_NAMESPACE::JniEnv::shutdown_ = false;


jint JNI_OnLoad( JavaVM* vm, void* /*reserved*/ ) {
  std::cerr << "JNI_OnLoad " << vm << '\n';
  return JNI_VERSION_1_6;
}


void JNI_OnUnload( JavaVM* vm, void* /*reserved*/ ) {
  std::cerr << "JNI_OnUnload " << vm << '\n';
}
