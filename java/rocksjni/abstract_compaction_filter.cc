// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/compaction_filter.h"
#include "include/org_rocksdb_AbstractCompactionFilter.h"
#include "rocksjni/portal.h"


namespace ROCKSDB_NAMESPACE {

  class CompactionFilterJni final : public CompactionFilter {

  public:
    [[nodiscard]]
    static std::unique_ptr<CompactionFilterJni> from( JNIEnv* const env ) {
      JavaVM* jvm = nullptr;
      auto const rs = env->GetJavaVM( &jvm );
      if ( rs != JNI_OK || jvm == nullptr )
        return nullptr; // error

      auto const jclazz = env->FindClass("org/rocksdb/AbstractCompactionFilter");
      if ( jclazz == nullptr )
        return nullptr; // error

      auto const jmethodId = env->GetMethodID(jclazz, "filter", "([B[B)Z" );
      if ( jmethodId == nullptr )
        return nullptr; // error

      return std::unique_ptr<CompactionFilterJni>{
        new CompactionFilterJni( jvm, jmethodId )
      };
    }

  public:
    CompactionFilterJni( const CompactionFilterJni& ) = delete;
    CompactionFilterJni( CompactionFilterJni&& ) = delete;

    ~CompactionFilterJni() final {
      if ( jself_ != nullptr ) {
        const auto& env = JniEnv::fast( jvm_ );
        if ( env )
          env->DeleteGlobalRef( jself_ );
      }
    }

  public:
    CompactionFilterJni& operator=( const CompactionFilterJni& ) = delete;
    CompactionFilterJni& operator=( CompactionFilterJni&& ) = delete;

  public:
    [[nodiscard]]
    const char* Name() const final {
      return "CompactionFilterJni";
    }

    bool Filter( int /*level*/, const Slice& key,
                 const Slice& existing_value,
                 std::string* /*new_value*/,
                 bool* /*value_changed*/ ) const final {
      const auto& env = JniEnv::fast( jvm_ );
      if ( !env )
        return false; // error

      auto const jkey = JniUtil::copyBytes( env.get(), key );
      if ( jkey == nullptr )
        return false; // error
      auto const jexisting_value = JniUtil::copyBytes( env.get(), existing_value );
      if ( jexisting_value == nullptr )
        return false; // error

      auto const jresult = env->CallBooleanMethod(
        jself_, jmethodId_,
        jkey, jexisting_value
      );

      if ( env->ExceptionCheck() ) {
        // FIXME: I don't know what to do with the exception
        return false; // error
      }

       // delete local reference is not mandatory but jnicheck emits warnings
      env->DeleteLocalRef( jexisting_value );
      env->DeleteLocalRef( jkey );
      return jresult == JNI_TRUE;
    }

  public:
    bool setSelf( jobject jthis ) {
      if ( jself_ != nullptr || jthis == nullptr )
        return false; // error
      const auto& env = JniEnv::fast( jvm_ );
      if ( !env )
        return false; // error
      auto const jself = env->NewGlobalRef(jthis );
      if ( jself == nullptr )
        return false; // error
      jself_ = jself;
      return true;
    }

  private:
    CompactionFilterJni( JavaVM* const jvm, jmethodID jmethodId ) noexcept
      : jvm_(jvm), jmethodId_(jmethodId), jself_(nullptr) { }

  private:
    JavaVM* const jvm_;
    jmethodID jmethodId_;
    jobject jself_;
  };

} // namespace ROCKSDB_NAMESPACE

// ---------------------------------------------------------------------------------------------------------------------------------

jlong Java_org_rocksdb_AbstractCompactionFilter_newOperator( JNIEnv* env, jclass /*jclazz*/ ) {
  return GET_CPLUSPLUS_POINTER( new std::shared_ptr<ROCKSDB_NAMESPACE::CompactionFilterJni>(
    ROCKSDB_NAMESPACE::CompactionFilterJni::from( env )
  ));
}

void Java_org_rocksdb_AbstractCompactionFilter_disposeInternal( JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle ) {
  auto* const shared_ptr = reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::CompactionFilterJni>*>( jhandle );
  delete shared_ptr;
}

jboolean Java_org_rocksdb_AbstractCompactionFilter_initOperator( JNIEnv* /*env*/, jobject jthis, jlong jhandle ) {
  auto* const shared_ptr = reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::CompactionFilterJni>*>( jhandle );
  if ( shared_ptr == nullptr || !*shared_ptr )
    return false; // error
  return (*shared_ptr)->setSelf( jthis );
}
