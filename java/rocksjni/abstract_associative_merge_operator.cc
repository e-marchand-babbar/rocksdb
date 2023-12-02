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
      JavaVM* jvm = nullptr;
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
        std::cerr << "Destroying AssociativeMergeOperatorJni [self: " << jself_ << ']' << '\n';
        const auto& env = JniEnv::getOrCreate( jvm_ );
        //auto const env = JniEnv::from( jvm_ );
        if ( env ) {
          env->DeleteGlobalRef(jself_);
          std::cerr << "Destroyed AssociativeMergeOperatorJni [self: " << jself_ << ']' << '\n';
        }
      }
    }

  public:
    AssociativeMergeOperatorJni& operator=( const AssociativeMergeOperatorJni& ) = delete;
    AssociativeMergeOperatorJni& operator=( AssociativeMergeOperatorJni&& ) = delete;

  public:
    [[nodiscard]]
    const char* Name() const final {
      return "AssociativeMergeOperatorJni";
    }

    bool Merge( const Slice& key,
                const Slice* existing_value,
                const Slice& value,
                std::string* new_value,
                Logger* /*logger*/ ) const final {
      const auto& env = JniEnv::getOrCreate( jvm_ );
      //auto const env = JniEnv::from( jvm_ );
      if ( !env )
        return false; // error

      auto const jkey = JniUtil::copyBytes( env.get(), key );
      if ( jkey == nullptr )
        return false; // error
      auto const jexisting_value = existing_value != nullptr
        ? JniUtil::copyBytes( env.get(), *existing_value )
        : nullptr;
      if ( existing_value != nullptr && jexisting_value == nullptr )
        return false; // error
      auto const jvalue = JniUtil::copyBytes( env.get(), value );
      if ( jvalue == nullptr )
        return false; // error

      auto const jresult = (jbyteArray) env->CallObjectMethod(
        jself_, jmethodId_,
        jkey, jexisting_value, jvalue
      );

      if ( env->ExceptionCheck() || jresult == nullptr ) {
        // FIXME: I don't know what to do with the exception
        return false; // error
      }

      auto const len = static_cast<size_t>( env->GetArrayLength(jresult) / sizeof(char) );
      auto* const carray = env->GetPrimitiveArrayCritical( jresult, nullptr );
      if ( carray == nullptr )
        return false; // error
      new_value->assign( reinterpret_cast<const char*>(carray), len );
      env->ReleasePrimitiveArrayCritical( jresult, carray, JNI_ABORT );
      env->DeleteLocalRef( jresult );
      env->DeleteLocalRef( jvalue );
      env->DeleteLocalRef( jexisting_value );
      env->DeleteLocalRef( jkey );
      return true;
    }

  public:
    bool setSelf( jobject jthis ) {
      if ( jself_ != nullptr || jthis == nullptr )
        return false; // error
      const auto& env = JniEnv::getOrCreate( jvm_ );
      //auto const env = JniEnv::from( jvm_ );
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
  return (*shared_ptr)->setSelf( jthis );
}

