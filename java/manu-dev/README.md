# How-to build and edit RocksDB JNI

## Build RocksDB JNI
* `make -j 8 rocksdbjavastatic` should be enough
  * you should find output files in `java/target/`


* in the `java/` directory, one could:
  * `make clean`
  * `make java` generate JNI header files from Java sources
  * `make java_test` generate JNI header files from Java tests
  * `make test` generate all JNI header files and run tests

## Edit RocksDB JNI
* using IntelliJ IDEA for the Java sources:
  * `File > New > Project from Existing Sources...`
  * select `java/` directory then default options
  * `Project Structure... > Modules > test > Dependencies > + > Module Dependency...`
  * add module `main` as dependency for `test`


* using CLion for the C++ sources:
  * open `java/manu-dev/CMakeLists.txt` as project
  * adjust build options (e.g. `-j 8`)

# Make files
  * `./src.mk`
  * `./Makefile`
  * `./java/CMakeLists.txt`
  * `./java/Makefile`
  * `./java/manu-dev/CMakeLists.txt`
    * This is a convenience project to aid the development of JNI

# Source files
  * `./java/rocksjni/`
  * `./java/src/`