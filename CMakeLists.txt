cmake_minimum_required(VERSION 3.13)

project(partition_for_join_emulation)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -pg")
set(PROJECT_SOURCE_DIR ./src/)

add_subdirectory(${PROJECT_SOURCE_DIR}/hash)

add_executable(estm src/estimator_main.cc src/estimator.cc)
add_executable(emul src/emulator_main.cc src/emulator.cc src/estimator.cc)
add_executable(load-gen src/workload_generator.cc src/dist_generator.cc)

target_link_libraries(estm PUBLIC Hash)
target_link_libraries(emul PUBLIC Hash)

target_include_directories(estm PUBLIC ${PROJECT_SOURCE_DIR}/hash)
target_include_directories(emul PUBLIC ${PROJECT_SOURCE_DIR}/hash)
