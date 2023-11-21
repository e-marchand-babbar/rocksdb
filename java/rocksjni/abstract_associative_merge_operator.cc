/*
 * @author cristian.lorenzetto@gmail.com
 * @author emmanuel.marchand@babbar.tech
 */

#include <string>
#include "rocksdb/merge_operator.h"
#include "include/org_rocksdb_AbstractAssociativeMergeOperator.h"
#include "rocksjni/portal.h"


namespace ROCKSDB_NAMESPACE {

  class AssociativeMergeOperatorJni final : public AssociativeMergeOperator {

  public:
    static std::unique_ptr<AssociativeMergeOperatorJni> from( JNIEnv* const env ) {
      JavaVM* jvm{ nullptr };
      auto const rs = env->GetJavaVM( &jvm );
      if ( rs != JNI_OK || jvm == nullptr )
        return nullptr; // error

      auto const jclazz = env->FindClass("org/rocksdb/AbstractAssociativeMergeOperator");
      if ( jclazz == nullptr )
        return nullptr; // error

      auto const jmethodId = env->GetMethodID(jclazz, "merge", "([B[B[B)[B" );
      if ( jmethodId == nullptr )
        return nullptr; // error

      return std::unique_ptr<AssociativeMergeOperatorJni>{
        new AssociativeMergeOperatorJni( jvm, jmethodId )
      };
    }

  public:
    AssociativeMergeOperatorJni( const AssociativeMergeOperatorJni& ) = delete;
    AssociativeMergeOperatorJni( AssociativeMergeOperatorJni&& ) = delete;

    ~AssociativeMergeOperatorJni() final {
      if ( jself_ != nullptr ) {
        auto const env = JniEnv::from( jvm_ );
        if ( env )
          env->DeleteGlobalRef(jself_ );
      }
    }

  public:
    AssociativeMergeOperatorJni& operator=( const AssociativeMergeOperatorJni& ) = delete;
    AssociativeMergeOperatorJni& operator=( AssociativeMergeOperatorJni&& ) = delete;

  public:
    [[nodiscard]]
    const char* Name() const final {
      return "JNIAbstractAssociativeMergeOperator";
    }

    bool Merge( const Slice& key,
                const Slice* existing_value,
                const Slice& value,
                std::string* new_value,
                Logger* /*logger*/ ) const final {
      // TODO: better error handling (OOM, etc.)
      auto const env = JniEnv::from( jvm_ );
      if ( !env )
        return false; // error

      auto const jkey = env.clone_slice( key );
      auto const jexisting_value = existing_value
        ? env.clone_slice( *existing_value )
        : nullptr;
      auto const jvalue = env.clone_slice( value );

      auto const jresult = (jbyteArray) env->CallObjectMethod(
        jself_, jmethodId_,
        jkey, jexisting_value, jvalue
      );

      if ( env->ExceptionCheck() || jresult == nullptr ) {
        auto const exn = env->ExceptionOccurred();
        env->DeleteLocalRef( jresult );
        env->DeleteLocalRef( jvalue );
        env->DeleteLocalRef( jexisting_value );
        env->DeleteLocalRef( jkey );
        if ( exn != nullptr )
          env->Throw( exn );
        return false;
      }

      auto const len = static_cast<size_t>( env->GetArrayLength(jresult) / sizeof(char) );
      auto* const carray = env->GetPrimitiveArrayCritical( jresult, nullptr );
      new_value->assign( reinterpret_cast<const char *>(carray), len );
      env->ReleasePrimitiveArrayCritical( jresult, carray, JNI_ABORT );

      env->DeleteLocalRef( jresult );
      env->DeleteLocalRef( jvalue );
      env->DeleteLocalRef( jexisting_value );
      env->DeleteLocalRef( jkey );
      return true;
    }

  public:
    bool set_self( jobject jthis ) {
      if ( jthis == nullptr )
        return false; // error
      auto const env = JniEnv::from( jvm_ );
      if ( !env )
        return false; // error
      auto const jself = env->NewGlobalRef(jthis );
      if ( jself == nullptr )
        return false; // error
      jself_ = jself;
      return true;
    }

  private:
    AssociativeMergeOperatorJni( JavaVM* const jvm, jmethodID jmethodId ) noexcept
      : jvm_(jvm), jmethodId_(jmethodId), jself_(nullptr) { }

  private:
    JavaVM* const jvm_;
    jmethodID jmethodId_;
    jobject jself_;
  };

} // namespace ROCKSDB_NAMESPACE

// ---------------------------------------------------------------------------------------------------------------------------------

jlong Java_org_rocksdb_AbstractAssociativeMergeOperator_newOperator( JNIEnv* env, jclass /*jclazz*/ ) {
  return GET_CPLUSPLUS_POINTER( new std::shared_ptr<ROCKSDB_NAMESPACE::MergeOperator>(
    ROCKSDB_NAMESPACE::AssociativeMergeOperatorJni::from( env )
  ));
}

void Java_org_rocksdb_AbstractAssociativeMergeOperator_disposeInternal( JNIEnv* /*env*/, jobject /*obj*/, jlong jhandle ) {
  auto* const shared_ptr = reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::MergeOperator>*>( jhandle );
  delete shared_ptr;
}

jboolean Java_org_rocksdb_AbstractAssociativeMergeOperator_initOperator( JNIEnv* /*env*/, jobject jthis, jlong jhandle ) {
  auto* const shared_ptr = reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::AssociativeMergeOperatorJni>*>( jhandle );
  if ( shared_ptr == nullptr || !*shared_ptr )
    return false; // error
  return (*shared_ptr)->set_self(jthis );
}
