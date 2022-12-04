cmake_minimum_required(VERSION 3.20)
project(mvcc)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_BUILD_TYPE Release)

include_directories(/usr/local/include)
link_directories(/usr/local/lib)

set(MVCC_SRCS OpCoordinator.cpp OpCoordinator.h
        Operation.cpp Operation.h
        Value.cpp Value.h
        ValueTable.cpp ValueTable.h
        Version.cpp Version.h
        SkipList.h SkipList.cpp
        )

add_executable(mvcc main.cpp
        ${MVCC_SRCS} )

option(BUILD_TEST "BUILD TEST OPTION" ON)

if(DEFINED BUILD_TEST)
    enable_testing()
    message(STATUS "BUILD TEST: " ${BUILD_TEST})
endif()

if(BUILD_TEST)
    add_executable(mvcc_test
            test/test.cpp ${MVCC_SRCS}
            test/speed_test.cpp)
    target_link_libraries(mvcc_test gtest gtest_main)
    add_test(NAME MVCC_TEST COMMAND mvcc_test)
endif()
