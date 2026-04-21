option(ENABLE_ONNXRUNTIME "Enable ONNX Runtime (required for Gemma embedding functions)" ${ENABLE_LIBRARIES})

if (NOT ENABLE_ONNXRUNTIME)
    message(STATUS "Not using ONNX Runtime")
    return()
endif()

if (SANITIZE STREQUAL "thread")
    message(STATUS "Not using ONNX Runtime with Thread Sanitizer (incompatible)")
    set(ENABLE_ONNXRUNTIME OFF CACHE BOOL "" FORCE)
    return()
endif()

set(ORT_SRC "${ClickHouse_SOURCE_DIR}/contrib/onnxruntime")
set(ORT_CMAKE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
set(ORT_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${ORT_GEN_DIR}/onnx")

# Generate onnxruntime_config.h from template
set(ORT_VERSION "1.25.0")
set(ORT_BUILD_INFO "ORT source build")
configure_file(
    "${ORT_SRC}/cmake/onnxruntime_config.h.in"
    "${ORT_GEN_DIR}/onnxruntime_config.h"
    @ONLY)

# options-pinned.h is not a template — copy from ORT source if it exists,
# otherwise create a minimal stub (it's only used for ORT's own option pinning).
if (NOT EXISTS "${ORT_GEN_DIR}/options-pinned.h")
    file(WRITE "${ORT_GEN_DIR}/options-pinned.h" "// Generated stub\n#pragma once\n")
endif()

if (NOT EXISTS "${ORT_SRC}/onnxruntime/core/session/inference_session.cc")
    message(STATUS "ONNX Runtime source not found at ${ORT_SRC}, skipping Gemma functions")
    return()
endif()

set(ORT_SRCS
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/activations.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/attnlstm/attention_wrapper.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/attnlstm/bahdanau_attention.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/attnlstm/deep_cpu_attn_lstm.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/attnlstm/uni_dir_attn_lstm.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/bert/attention.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/bert/attention_utils.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/bert/bias_gelu.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/bert/bifurcation_detector.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/bert/decoder_masked_multihead_attention.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/bert/embed_layer_norm.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/bert/fast_gelu.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/bert/group_query_attention.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/bert/multihead_attention.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/bert/ngram_repeat_block.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/bert/rotary_embedding.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/cdist.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/conv_transpose_with_dynamic_pads.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/cpu_contrib_kernels.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/crop.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/crop_and_resize.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/dynamicslice.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/element_wise_ops.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/expand_dims.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/fused_activation.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/fused_conv.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/fused_gemm.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/fused_matmul.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/grid_sample.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/image_scaler.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/inverse.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/layer_norm.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/math/sparse_dense_matmul.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/matmul_fpq4.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/maxpool_with_mask.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/mean_variance_normalization_exp.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/moe/moe_cpu.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/moe/moe_quantization_cpu.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/moe/moe_utils.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/murmur_hash3.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/nchwc_ops.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/quantization/attention_quant.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/quantization/dynamic_quantize_lstm.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/quantization/dynamic_quantize_matmul.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/quantization/gather_block_quantized.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/quantization/matmul_bnb4.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/quantization/matmul_integer16.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/quantization/matmul_nbits.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/quantization/matmul_nbits_impl.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/quantization/nhwc_max_pool.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/quantization/qembed_layer_norm.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/quantization/qlinear_activations.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/quantization/qlinear_binary_op.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/quantization/qlinear_concat.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/quantization/qlinear_global_average_pool.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/quantization/qlinear_lookup_table.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/quantization/qlinear_pool.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/quantization/qlinear_softmax.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/quantization/qlinear_where.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/quantization/quant_gemm.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/sample.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/skip_layer_norm.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/sparse/sparse_attention.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/tensor/dynamic_time_warping.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/tensor/shrunken_gather.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/tensor/unfold.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/tokenizer.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/transformers/beam_search.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/transformers/beam_search_parameters.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/transformers/beam_search_scorer.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/transformers/generation_device_helper.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/transformers/greedy_search.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/transformers/greedy_search_parameters.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/transformers/logits_processor.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/transformers/sampling.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/transformers/sampling_parameters.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/transformers/sequences.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/transformers/subgraph_base.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/transformers/subgraph_gpt.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/transformers/subgraph_t5_decoder.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/transformers/subgraph_t5_encoder.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/transformers/subgraph_whisper_decoder.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/transformers/subgraph_whisper_encoder.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/unique.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/utils/dump_tensor.cc"
    "${ORT_SRC}/onnxruntime/contrib_ops/cpu/word_conv_embedding.cc"
    "${ORT_SRC}/onnxruntime/core/common/cpuid_info.cc"
    "${ORT_SRC}/onnxruntime/core/common/cpuid_info_vendor.cc"
    "${ORT_SRC}/onnxruntime/core/common/cpuid_uarch.cc"
    "${ORT_SRC}/onnxruntime/core/common/denormal.cc"
    "${ORT_SRC}/onnxruntime/core/common/helper.cc"
    "${ORT_SRC}/onnxruntime/core/common/logging/capture.cc"
    "${ORT_SRC}/onnxruntime/core/common/logging/logging.cc"
    "${ORT_SRC}/onnxruntime/core/common/logging/sinks/ostream_sink.cc"
    "${ORT_SRC}/onnxruntime/core/common/profiler.cc"
    "${ORT_SRC}/onnxruntime/core/common/profiler_common.cc"
    "${ORT_SRC}/onnxruntime/core/common/semver.cc"
    "${ORT_SRC}/onnxruntime/core/common/spin_pause.cc"
    "${ORT_SRC}/onnxruntime/core/common/status.cc"
    "${ORT_SRC}/onnxruntime/core/common/threadpool.cc"
    "${ORT_SRC}/onnxruntime/core/flatbuffers/flatbuffers_utils.cc"
    "${ORT_SRC}/onnxruntime/core/framework/allocation_planner.cc"
    "${ORT_SRC}/onnxruntime/core/framework/allocator.cc"
    "${ORT_SRC}/onnxruntime/core/framework/allocator_utils.cc"
    "${ORT_SRC}/onnxruntime/core/framework/bfc_arena.cc"
    "${ORT_SRC}/onnxruntime/core/framework/callback.cc"
    "${ORT_SRC}/onnxruntime/core/framework/config_options.cc"
    "${ORT_SRC}/onnxruntime/core/framework/copy.cc"
    "${ORT_SRC}/onnxruntime/core/framework/customregistry.cc"
    "${ORT_SRC}/onnxruntime/core/framework/data_transfer.cc"
    "${ORT_SRC}/onnxruntime/core/framework/data_transfer_manager.cc"
    "${ORT_SRC}/onnxruntime/core/framework/data_types.cc"
    "${ORT_SRC}/onnxruntime/core/framework/debug_node_inputs_outputs_utils.cc"
    "${ORT_SRC}/onnxruntime/core/framework/device_stream_collection.cc"
    "${ORT_SRC}/onnxruntime/core/framework/empty.cc"
    "${ORT_SRC}/onnxruntime/core/framework/endian_utils.cc"
    "${ORT_SRC}/onnxruntime/core/framework/ep_context_options.cc"
    "${ORT_SRC}/onnxruntime/core/framework/ep_context_utils.cc"
    "${ORT_SRC}/onnxruntime/core/framework/error_code.cc"
    "${ORT_SRC}/onnxruntime/core/framework/ex_lib_loader.cc"
    "${ORT_SRC}/onnxruntime/core/framework/execution_frame.cc"
    "${ORT_SRC}/onnxruntime/core/framework/execution_provider.cc"
    "${ORT_SRC}/onnxruntime/core/framework/execution_steps.cc"
    "${ORT_SRC}/onnxruntime/core/framework/external_data_loader.cc"
    "${ORT_SRC}/onnxruntime/core/framework/external_data_loader_manager.cc"
    "${ORT_SRC}/onnxruntime/core/framework/fallback_cpu_capability.cc"
    "${ORT_SRC}/onnxruntime/core/framework/feeds_fetches_manager.cc"
    "${ORT_SRC}/onnxruntime/core/framework/func_kernel.cc"
    "${ORT_SRC}/onnxruntime/core/framework/fuse_nodes_funcs.cc"
    "${ORT_SRC}/onnxruntime/core/framework/graph_partitioner.cc"
    "${ORT_SRC}/onnxruntime/core/framework/kernel_def_builder.cc"
    "${ORT_SRC}/onnxruntime/core/framework/kernel_registry.cc"
    "${ORT_SRC}/onnxruntime/core/framework/kernel_registry_manager.cc"
    "${ORT_SRC}/onnxruntime/core/framework/kernel_type_str_resolver.cc"
    "${ORT_SRC}/onnxruntime/core/framework/kernel_type_str_resolver_utils.cc"
    "${ORT_SRC}/onnxruntime/core/framework/library_handles.cc"
    "${ORT_SRC}/onnxruntime/core/framework/memcpy.cc"
    "${ORT_SRC}/onnxruntime/core/framework/memory_info.cc"
    "${ORT_SRC}/onnxruntime/core/framework/mldata_type_utils.cc"
    "${ORT_SRC}/onnxruntime/core/framework/model_metadef_id_generator.cc"
    "${ORT_SRC}/onnxruntime/core/framework/murmurhash3.cc"
    "${ORT_SRC}/onnxruntime/core/framework/node_index_info.cc"
    "${ORT_SRC}/onnxruntime/core/framework/node_unit.cc"
    "${ORT_SRC}/onnxruntime/core/framework/onnxruntime_map_type_info.cc"
    "${ORT_SRC}/onnxruntime/core/framework/onnxruntime_optional_type_info.cc"
    "${ORT_SRC}/onnxruntime/core/framework/onnxruntime_sequence_type_info.cc"
    "${ORT_SRC}/onnxruntime/core/framework/onnxruntime_typeinfo.cc"
    "${ORT_SRC}/onnxruntime/core/framework/op_kernel.cc"
    "${ORT_SRC}/onnxruntime/core/framework/op_kernel_info.cc"
    "${ORT_SRC}/onnxruntime/core/framework/op_node_proto_helper.cc"
    "${ORT_SRC}/onnxruntime/core/framework/ort_value_pattern_planner.cc"
    "${ORT_SRC}/onnxruntime/core/framework/ort_value_tensor_slicer.cc"
    "${ORT_SRC}/onnxruntime/core/framework/partial_graph_execution_state.cc"
    "${ORT_SRC}/onnxruntime/core/framework/plugin_data_transfer.cc"
    "${ORT_SRC}/onnxruntime/core/framework/plugin_ep_stream.cc"
    "${ORT_SRC}/onnxruntime/core/framework/prepacked_weights.cc"
    "${ORT_SRC}/onnxruntime/core/framework/prepacked_weights_container.cc"
    "${ORT_SRC}/onnxruntime/core/framework/random_generator.cc"
    "${ORT_SRC}/onnxruntime/core/framework/random_seed.cc"
    "${ORT_SRC}/onnxruntime/core/framework/resource_accountant.cc"
    "${ORT_SRC}/onnxruntime/core/framework/run_options.cc"
    "${ORT_SRC}/onnxruntime/core/framework/sequential_executor.cc"
    "${ORT_SRC}/onnxruntime/core/framework/session_options.cc"
    "${ORT_SRC}/onnxruntime/core/framework/session_state.cc"
    "${ORT_SRC}/onnxruntime/core/framework/session_state_utils.cc"
    "${ORT_SRC}/onnxruntime/core/framework/simple_tensor_allocator.cc"
    "${ORT_SRC}/onnxruntime/core/framework/sparse_tensor.cc"
    "${ORT_SRC}/onnxruntime/core/framework/sparse_utils.cc"
    "${ORT_SRC}/onnxruntime/core/framework/stream_execution_context.cc"
    "${ORT_SRC}/onnxruntime/core/framework/stream_handles.cc"
    "${ORT_SRC}/onnxruntime/core/framework/tensor.cc"
    "${ORT_SRC}/onnxruntime/core/framework/tensor_allocator.cc"
    "${ORT_SRC}/onnxruntime/core/framework/tensor_external_data_info.cc"
    "${ORT_SRC}/onnxruntime/core/framework/tensor_shape.cc"
    "${ORT_SRC}/onnxruntime/core/framework/tensor_type_and_shape.cc"
    "${ORT_SRC}/onnxruntime/core/framework/tensorprotoutils.cc"
    "${ORT_SRC}/onnxruntime/core/framework/transpose_helper.cc"
    "${ORT_SRC}/onnxruntime/core/framework/utils.cc"
    "${ORT_SRC}/onnxruntime/core/graph/contrib_ops/attn_lstm_schema_defs.cc"
    "${ORT_SRC}/onnxruntime/core/graph/contrib_ops/bert_defs.cc"
    "${ORT_SRC}/onnxruntime/core/graph/contrib_ops/collective_defs.cc"
    "${ORT_SRC}/onnxruntime/core/graph/contrib_ops/contrib_defs.cc"
    "${ORT_SRC}/onnxruntime/core/graph/contrib_ops/diffusion_defs.cc"
    "${ORT_SRC}/onnxruntime/core/graph/contrib_ops/internal_nhwc_onnx_schemas.cc"
    "${ORT_SRC}/onnxruntime/core/graph/contrib_ops/nchwc_schema_defs.cc"
    "${ORT_SRC}/onnxruntime/core/graph/contrib_ops/nhwc_schema_defs.cc"
    "${ORT_SRC}/onnxruntime/core/graph/contrib_ops/onnx_deprecated_operators.cc"
    "${ORT_SRC}/onnxruntime/core/graph/contrib_ops/onnx_function_util.cc"
    "${ORT_SRC}/onnxruntime/core/graph/contrib_ops/quantization_defs.cc"
    "${ORT_SRC}/onnxruntime/core/graph/contrib_ops/range_schema_defs.cc"
    "${ORT_SRC}/onnxruntime/core/graph/contrib_ops/shape_inference_functions.cc"
    "${ORT_SRC}/onnxruntime/core/graph/data_propagation/add_op_data_propagation.cc"
    "${ORT_SRC}/onnxruntime/core/graph/data_propagation/custom_data_propagation.cc"
    "${ORT_SRC}/onnxruntime/core/graph/data_propagation/div_op_data_propagation.cc"
    "${ORT_SRC}/onnxruntime/core/graph/data_propagation/gather_op_data_propagation.cc"
    "${ORT_SRC}/onnxruntime/core/graph/data_propagation/mul_op_data_propagation.cc"
    "${ORT_SRC}/onnxruntime/core/graph/data_propagation/size_op_data_propagation.cc"
    "${ORT_SRC}/onnxruntime/core/graph/data_propagation/squeeze_op_data_propagation.cc"
    "${ORT_SRC}/onnxruntime/core/graph/data_propagation/sub_op_data_propagation.cc"
    "${ORT_SRC}/onnxruntime/core/graph/data_propagation/unsqueeze_op_data_propagation.cc"
    "${ORT_SRC}/onnxruntime/core/graph/ep_api_types.cc"
    "${ORT_SRC}/onnxruntime/core/graph/function.cc"
    "${ORT_SRC}/onnxruntime/core/graph/function_utils.cc"
    "${ORT_SRC}/onnxruntime/core/graph/graph.cc"
    "${ORT_SRC}/onnxruntime/core/graph/graph_flatbuffers_utils.cc"
    "${ORT_SRC}/onnxruntime/core/graph/graph_proto_serializer.cc"
    "${ORT_SRC}/onnxruntime/core/graph/graph_utils.cc"
    "${ORT_SRC}/onnxruntime/core/graph/graph_viewer.cc"
    "${ORT_SRC}/onnxruntime/core/graph/model.cc"
    "${ORT_SRC}/onnxruntime/core/graph/node_attr_utils.cc"
    "${ORT_SRC}/onnxruntime/core/graph/op.cc"
    "${ORT_SRC}/onnxruntime/core/graph/op_identifier_utils.cc"
    "${ORT_SRC}/onnxruntime/core/graph/runtime_optimization_record_container.cc"
    "${ORT_SRC}/onnxruntime/core/graph/schema_registry.cc"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/activate.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/activate_fp16.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/cast.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/compute.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/convolve.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/convsym.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/dequantize.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/dgemm.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/dwconv.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/eltwise.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/erf.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/flashattn.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/gelu.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/halfgemm.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/intrinsics/avx/min_max_elements.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/intrinsics/avx2/qdwconv_avx2.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/intrinsics/avx2/qladd_avx2.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/intrinsics/avx2/saturation_check_avx2.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/intrinsics/avx512/gelu_avx512f.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/intrinsics/avx512/quantize_avx512f.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/intrinsics/avx512/silu_avx512f.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/logistic.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/platform.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/pooling.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/pooling_fp16.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/q4_dq.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/q4_dq_cli.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/q4gemm.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/q4gemm_avx512.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/qdwconv.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/qdwconv_kernelsize.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/qgemm.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/qgemm_kernel_amx.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/qgemm_kernel_avx2.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/qgemm_kernel_default.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/qgemm_kernel_sse.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/qladd.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/qlgavgpool.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/qlmul.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/qlutgemm.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/qnbitgemm.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/qpostprocessor.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/quantize.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/reorder.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/rotary_embedding.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/rotary_embedding_kernel_avx2.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/saturation_check.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/sgemm.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/silu.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/snchwc.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/sqnbitgemm_kernel_avx2.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/sqnbitgemm_kernel_avx512.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/sqnbitgemm_kernel_avx512vnni.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/sqnbitgemm_lut_kernel_avx2.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/tanh.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/threading.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/transpose.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/ConvSymKernelAvx2.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/ConvSymKernelAvx512Core.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/DgemmKernelAvx.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/DgemmKernelAvx512F.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/DgemmKernelFma3.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/DgemmKernelSse2.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/ErfKernelFma3.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/LogisticKernelFma3.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/QgemmU8S8KernelAmx.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/QgemmU8S8KernelAmxCommon.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/QgemmU8S8KernelAvx2.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/QgemmU8U8KernelAvx2.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/QgemmU8X8KernelAvx2.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/QgemmU8X8KernelAvx512Core.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/QgemvU8S8KernelAvx2.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/QgemvU8S8KernelAvx512Core.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/QgemvU8S8KernelAvx512Vnni.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/QgemvU8S8KernelAvxVnni.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SconvKernelAvx.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SconvKernelAvx512F.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SconvKernelFma3.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SconvKernelSse2.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SgemmKernelAvx.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SgemmKernelAvx512F.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SgemmKernelFma3.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SgemmKernelM1Avx.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SgemmKernelM1TransposeBAvx.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SgemmKernelSse2.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SgemmTransposePackB16x4Avx.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SgemmTransposePackB16x4Sse2.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SoftmaxKernelAvx.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SoftmaxKernelAvx512F.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SpoolKernelAvx.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SpoolKernelAvx512F.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SpoolKernelSse2.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/TanhKernelFma3.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/TransKernelAvx512F.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/TransKernelFma3.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/cvtfp16a.S"
    "${ORT_SRC}/onnxruntime/core/optimizer/attention_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/bias_dropout_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/bias_gelu_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/bias_skip_layer_norm_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/bias_softmax_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/cast_chain_elimination.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/cast_elimination.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/common_subexpression_elimination.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/compute_optimizer/shared_utils.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/compute_optimizer/upstream_gather.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/compute_optimizer/upstream_gather_actors.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/compute_optimizer/upstream_reshape.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/compute_optimizer/upstream_reshape_actors.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/compute_optimizer/upstream_transformer_base.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/concat_slice_elimination.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/constant_folding.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/constant_sharing.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/conv_activation_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/conv_add_act_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/conv_add_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/conv_bn_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/conv_mul_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/div_mul_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/double_qdq_pairs_remover.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/dq_matmulnbits_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/dropout_elimination.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/dynamic_quantize_matmul_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/embed_layer_norm_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/expand_elimination.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/fast_gelu_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/free_dim_override_transformer.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/fuse_initializers_transformer.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/gather_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/gelu_approximation.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/gelu_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/gemm_activation_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/gemm_sum_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/gemm_transpose_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/graph_optimizer_registry.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/graph_transformer.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/graph_transformer_mgr.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/graph_transformer_utils.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/group_query_attention_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/identical_children_consolidation.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/identity_elimination.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/initializer.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/insert_cast_transformer.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/isinf_reducesum_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/label_encoder_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/layer_norm_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/layout_transformation/layout_transformation.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/matmul_activation_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/matmul_add_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/matmul_bn_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/matmul_integer_to_float.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/matmul_nbits_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/matmul_scale_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/matmul_transpose_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/nchwc_transformer.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/nhwc_transformer.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/noop_elimination.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/not_where_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/optimizer_execution_frame.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/pad_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/pre_shape_node_elimination.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/propagate_cast_ops.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/qdq_transformer/avx2_weight_s8_to_u8.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/qdq_transformer/clip_quantizelinear.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/qdq_transformer/constant_folding_dq_node.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/qdq_transformer/ensure_unique_dq_for_node_unit.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/qdq_transformer/qdq_final_cleanup.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/qdq_transformer/qdq_propagation.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/qdq_transformer/qdq_s8_to_u8.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/qdq_transformer/qdq_util.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/qdq_transformer/relu_quantizelinear.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/qdq_transformer/s8_to_u8.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/qdq_transformer/selectors_actions/qdq_actions.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/qdq_transformer/selectors_actions/qdq_selector_action_transformer.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/qdq_transformer/selectors_actions/qdq_selectors.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/qdq_transformer/selectors_actions/shared/utils.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/qdq_transformer/weight_bias_quantization.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/qdq_transformer/where_dummy_dq.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/quick_gelu_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/relu_clip_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/reshape_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/rule_based_graph_transformer.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/selection_and_optimization_func.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/selectors_actions/actions.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/selectors_actions/helpers.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/selectors_actions/selector_action_transformer.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/skip_layer_norm_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/slice_concat_to_space_to_depth_fusion.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/slice_elimination.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/stft_decomposition.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/transformer_memcpy.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/transpose_optimization/onnx_transpose_optimization.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/transpose_optimization/ort_optimizer_api_impl.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/transpose_optimization/ort_transpose_optimization.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/transpose_optimizer.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/unsqueeze_elimination.cc"
    "${ORT_SRC}/onnxruntime/core/optimizer/utils.cc"
    "${ORT_SRC}/onnxruntime/core/platform/check_intel.cc"
    "${ORT_SRC}/onnxruntime/core/platform/device_discovery_common.cc"
    "${ORT_SRC}/onnxruntime/core/platform/env.cc"
    "${ORT_SRC}/onnxruntime/core/platform/env_time.cc"
    "${ORT_SRC}/onnxruntime/core/platform/linux/device_discovery.cc"
    "${ORT_SRC}/onnxruntime/core/platform/logging/make_platform_default_log_sink.cc"
    "${ORT_SRC}/onnxruntime/core/platform/path_lib.cc"
    "${ORT_SRC}/onnxruntime/core/platform/posix/env.cc"
    "${ORT_SRC}/onnxruntime/core/platform/posix/env_time.cc"
    "${ORT_SRC}/onnxruntime/core/platform/posix/stacktrace.cc"
    "${ORT_SRC}/onnxruntime/core/platform/telemetry.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/activation/activations.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/controlflow/if.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/controlflow/loop.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/controlflow/scan_8.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/controlflow/scan_9.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/controlflow/scan_utils.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/controlflow/utils.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/cpu_execution_provider.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/cpu_provider_factory.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/cpu_provider_shared.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/fp16/fp16_conv.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/fp16/fp16_pool.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/generator/constant_of_shape.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/generator/random.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/generator/range.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/llm/attention.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/llm/rotary_embedding.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/llm/tensorscatter.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/math/clip.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/math/cumsum.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/math/det.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/math/einsum.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/math/einsum_utils/einsum_auxiliary_ops.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/math/einsum_utils/einsum_compute_preprocessor.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/math/einsum_utils/einsum_typed_compute_processor.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/math/element_wise_ops.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/math/gemm.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/math/hardmax.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/math/matmul.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/math/round.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/math/sign.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/math/softmax.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/math/softmax_shared.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/math/top_k.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/ml/array_feature_extractor.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/ml/binarizer.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/ml/cast_map.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/ml/category_mapper.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/ml/dictvectorizer.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/ml/feature_vectorizer.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/ml/imputer.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/ml/label_encoder.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/ml/linearclassifier.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/ml/linearregressor.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/ml/normalizer.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/ml/onehotencoder.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/ml/scaler.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/ml/svmclassifier.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/ml/svmregressor.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/ml/tree_ensemble.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/ml/tree_ensemble_classifier.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/ml/tree_ensemble_helper.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/ml/treeregressor.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/ml/zipmap.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/nn/Unpool.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/nn/batch_norm.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/nn/conv.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/nn/conv_transpose.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/nn/deform_conv.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/nn/dropout_op.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/nn/flatten.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/nn/instance_norm.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/nn/layer_norm.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/nn/layer_norm_impl.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/nn/lp_norm.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/nn/lrn.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/nn/pool.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/nn/rms_norm.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/nn/roi_pool.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/nn/shrink.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/nn/tfidfvectorizer.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/object_detection/non_max_suppression.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/object_detection/roialign.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/optional/optional_ops.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/quantization/conv_integer.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/quantization/dynamicquantizelinear.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/quantization/matmul_integer.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/quantization/qlinearconv.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/quantization/quantize_linear.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/quantization/quantize_linear_matmul.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/reduction/reduction_ops.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/rnn/deep_cpu_gru.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/rnn/deep_cpu_lstm.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/rnn/lstm_base.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/rnn/rnn.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/rnn/rnn_helpers.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/rnn/uni_directional_lstm.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/sequence/concat_from_sequence.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/sequence/sequence_ops.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/signal/dft.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/signal/window_functions.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/affine_grid.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/cast_op.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/col2im.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/compress.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/concat.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/expand.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/eye_like.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/gather.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/gather_elements.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/gather_nd.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/gelu.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/grid_sample.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/identity_op.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/isinf.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/isnan.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/mean_variance_normalization.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/nonzero_op.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/onehot.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/pad.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/reshape.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/resize.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/reverse_sequence.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/scatter.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/scatter_nd.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/shape_op.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/size.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/slice.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/space_depth_ops.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/split.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/squeeze.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/tile.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/transpose.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/trilu.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/unique.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/unsqueeze.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/upsample.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/tensor/where_op.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/text/regex_full_match.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/text/string_concat.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/text/string_normalizer.cc"
    "${ORT_SRC}/onnxruntime/core/providers/cpu/text/string_split.cc"
    "${ORT_SRC}/onnxruntime/core/providers/execution_provider_factory.cc"
    "${ORT_SRC}/onnxruntime/core/providers/get_execution_providers.cc"
    "${ORT_SRC}/onnxruntime/core/providers/partitioning_utils.cc"
    "${ORT_SRC}/onnxruntime/core/providers/shared/common.cc"
    "${ORT_SRC}/onnxruntime/core/providers/utils.cc"
    "${ORT_SRC}/onnxruntime/core/session/IOBinding.cc"
    "${ORT_SRC}/onnxruntime/core/session/abi_ep_types.cc"
    "${ORT_SRC}/onnxruntime/core/session/abi_session_options.cc"
    "${ORT_SRC}/onnxruntime/core/session/allocator_adapters.cc"
    "${ORT_SRC}/onnxruntime/core/session/compile_api.cc"
    "${ORT_SRC}/onnxruntime/core/session/custom_ops.cc"
    "${ORT_SRC}/onnxruntime/core/session/default_cpu_allocator_c_api.cc"
    "${ORT_SRC}/onnxruntime/core/session/environment.cc"
    "${ORT_SRC}/onnxruntime/core/session/inference_session.cc"
    "${ORT_SRC}/onnxruntime/core/session/inference_session_utils.cc"
    "${ORT_SRC}/onnxruntime/core/session/interop_api.cc"
    "${ORT_SRC}/onnxruntime/core/session/lora_adapters.cc"
    "${ORT_SRC}/onnxruntime/core/session/model_compilation_options.cc"
    "${ORT_SRC}/onnxruntime/core/session/model_editor_c_api.cc"
    "${ORT_SRC}/onnxruntime/core/session/onnxruntime_c_api.cc"
    "${ORT_SRC}/onnxruntime/core/session/ort_env.cc"
    "${ORT_SRC}/onnxruntime/core/session/plugin_ep/ep_api.cc"
    "${ORT_SRC}/onnxruntime/core/session/plugin_ep/ep_control_flow_kernel_impls.cc"
    "${ORT_SRC}/onnxruntime/core/session/plugin_ep/ep_factory_cpu.cc"
    "${ORT_SRC}/onnxruntime/core/session/plugin_ep/ep_factory_dml.cc"
    "${ORT_SRC}/onnxruntime/core/session/plugin_ep/ep_factory_internal.cc"
    "${ORT_SRC}/onnxruntime/core/session/plugin_ep/ep_factory_internal_impl.cc"
    "${ORT_SRC}/onnxruntime/core/session/plugin_ep/ep_factory_provider_bridge.cc"
    "${ORT_SRC}/onnxruntime/core/session/plugin_ep/ep_factory_webgpu.cc"
    "${ORT_SRC}/onnxruntime/core/session/plugin_ep/ep_kernel_registration.cc"
    "${ORT_SRC}/onnxruntime/core/session/plugin_ep/ep_library_internal.cc"
    "${ORT_SRC}/onnxruntime/core/session/plugin_ep/ep_library_plugin.cc"
    "${ORT_SRC}/onnxruntime/core/session/plugin_ep/ep_library_provider_bridge.cc"
    "${ORT_SRC}/onnxruntime/core/session/plugin_ep/ep_plugin_provider_interfaces.cc"
    "${ORT_SRC}/onnxruntime/core/session/provider_bridge_ort.cc"
    "${ORT_SRC}/onnxruntime/core/session/provider_policy_context.cc"
    "${ORT_SRC}/onnxruntime/core/session/provider_registration.cc"
    "${ORT_SRC}/onnxruntime/core/session/standalone_op_invoker.cc"
    "${ORT_SRC}/onnxruntime/core/session/utils.cc"
    "${ORT_SRC}/onnxruntime/core/util/math_cpu.cc"
    "${ORT_SRC}/onnxruntime/core/util/thread_utils.cc"
    "${ORT_SRC}/onnxruntime/lora/adapter_format_utils.cc"
)

set(ONNX_SRCS
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/checker.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/common/assertions.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/common/interned_strings.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/common/ir_pb_converter.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/common/model_helpers.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/common/status.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/attr_proto_util.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/controlflow/defs.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/controlflow/old.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/controlflow/utils.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/data_type_utils.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/function.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/generator/defs.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/generator/old.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/generator/utils.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/image/defs.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/logical/defs.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/logical/old.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/math/defs.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/math/old.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/math/utils.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/nn/defs.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/nn/old.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/nn/utils.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/object_detection/defs.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/object_detection/old.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/optional/defs.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/optional/old.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/parser.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/printer.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/quantization/defs.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/quantization/old.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/reduction/defs.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/reduction/old.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/reduction/utils.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/rnn/defs.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/rnn/old.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/schema.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/sequence/defs.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/sequence/old.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/sequence/utils.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/shape_inference.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/tensor/defs.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/tensor/old.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/tensor/utils.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/tensor_proto_util.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/tensor_util.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/text/defs.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/traditionalml/defs.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/traditionalml/old.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/defs/training/defs.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/inliner/inliner.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/shape_inference/implementation.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/version_converter/convert.cc"
    "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx/version_converter/helper.cc"
)

set(CPUINFO_SRCS
    "${ClickHouse_SOURCE_DIR}/contrib/pytorch_cpuinfo/src/api.c"
    "${ClickHouse_SOURCE_DIR}/contrib/pytorch_cpuinfo/src/cache.c"
    "${ClickHouse_SOURCE_DIR}/contrib/pytorch_cpuinfo/src/init.c"
    "${ClickHouse_SOURCE_DIR}/contrib/pytorch_cpuinfo/src/linux/cpulist.c"
    "${ClickHouse_SOURCE_DIR}/contrib/pytorch_cpuinfo/src/linux/multiline.c"
    "${ClickHouse_SOURCE_DIR}/contrib/pytorch_cpuinfo/src/linux/processors.c"
    "${ClickHouse_SOURCE_DIR}/contrib/pytorch_cpuinfo/src/linux/smallfile.c"
    "${ClickHouse_SOURCE_DIR}/contrib/pytorch_cpuinfo/src/log.c"
    "${ClickHouse_SOURCE_DIR}/contrib/pytorch_cpuinfo/src/x86/cache/descriptor.c"
    "${ClickHouse_SOURCE_DIR}/contrib/pytorch_cpuinfo/src/x86/cache/deterministic.c"
    "${ClickHouse_SOURCE_DIR}/contrib/pytorch_cpuinfo/src/x86/cache/init.c"
    "${ClickHouse_SOURCE_DIR}/contrib/pytorch_cpuinfo/src/x86/info.c"
    "${ClickHouse_SOURCE_DIR}/contrib/pytorch_cpuinfo/src/x86/init.c"
    "${ClickHouse_SOURCE_DIR}/contrib/pytorch_cpuinfo/src/x86/isa.c"
    "${ClickHouse_SOURCE_DIR}/contrib/pytorch_cpuinfo/src/x86/linux/cpuinfo.c"
    "${ClickHouse_SOURCE_DIR}/contrib/pytorch_cpuinfo/src/x86/linux/init.c"
    "${ClickHouse_SOURCE_DIR}/contrib/pytorch_cpuinfo/src/x86/name.c"
    "${ClickHouse_SOURCE_DIR}/contrib/pytorch_cpuinfo/src/x86/topology.c"
    "${ClickHouse_SOURCE_DIR}/contrib/pytorch_cpuinfo/src/x86/uarch.c"
    "${ClickHouse_SOURCE_DIR}/contrib/pytorch_cpuinfo/src/x86/vendor.c"
)

# Generate protobuf files from onnx/*.proto using ClickHouse's protoc
set(ONNX_PROTO_DIR "${ClickHouse_SOURCE_DIR}/contrib/onnx/onnx")
set(ONNX_PROTOS
    onnx-ml.proto
    onnx-operators-ml.proto
    onnx-data.proto)

set(ONNX_PROTO_SRCS "")
foreach(proto ${ONNX_PROTOS})
    get_filename_component(proto_name ${proto} NAME_WE)
    set(pb_cc "${ORT_GEN_DIR}/onnx/${proto_name}.pb.cc")
    set(pb_h  "${ORT_GEN_DIR}/onnx/${proto_name}.pb.h")
    add_custom_command(
        OUTPUT ${pb_cc} ${pb_h}
        COMMAND $<TARGET_FILE:protobuf::protoc>
            --proto_path=${ClickHouse_SOURCE_DIR}/contrib/onnx
            --cpp_out=${ORT_GEN_DIR}
            ${ONNX_PROTO_DIR}/${proto}
        DEPENDS ${ONNX_PROTO_DIR}/${proto} protobuf::protoc
        COMMENT "Generating ${proto_name}.pb.cc/h"
        VERBATIM)
    list(APPEND ONNX_PROTO_SRCS ${pb_cc})
endforeach()

# ── pytorch_cpuinfo — build separately ─────────────────────────────────────
set(CPUINFO_DIR "${ClickHouse_SOURCE_DIR}/contrib/pytorch_cpuinfo")
add_library(_ort_cpuinfo STATIC ${CPUINFO_SRCS})
target_include_directories(_ort_cpuinfo PRIVATE
    "${CPUINFO_DIR}/include"
    "${CPUINFO_DIR}/src")
target_compile_options(_ort_cpuinfo PRIVATE -D_GNU_SOURCE)
target_compile_definitions(_ort_cpuinfo PRIVATE
    CPUINFO_LOG_LEVEL=2
    CPUINFO_LOG_TO_STDIO=1
    CPUINFO_SUPPORTED
    CPUINFO_BUILD_MOCK_TESTS=0)

# ── onnx schema ─────────────────────────────────────────────────────────────
add_library(_ort_onnx STATIC ${ONNX_SRCS} ${ONNX_PROTO_SRCS})
target_include_directories(_ort_onnx SYSTEM PRIVATE
    "${ClickHouse_SOURCE_DIR}/contrib/onnx"
    "${ORT_GEN_DIR}"        # root of generated/ (contains onnx/ subdir with pb.h)
    "${ClickHouse_SOURCE_DIR}/contrib/google-protobuf/src"
    "${ClickHouse_SOURCE_DIR}/contrib/abseil-cpp")
target_compile_definitions(_ort_onnx PRIVATE
    ONNX_ML=1
    ONNX_NAMESPACE=onnx
    ONNX_USE_LITE_PROTO=1
    __ONNX_DISABLE_STATIC_REGISTRATION
    __ONNX_NO_DOC_STRINGS
    ORT_NO_RTTI
    _GNU_SOURCE)
target_link_libraries(_ort_onnx PRIVATE ch_contrib::protobuf)

# ── Main ORT library ─────────────────────────────────────────────────────────
add_library(_onnxruntime STATIC ${ORT_SRCS})
# Disable large object size check for ORT — some ORT files (e.g. element_wise_ops.cc)
# compile to large .o files due to template instantiations.
set_target_properties(_onnxruntime PROPERTIES SKIP_CHECK_LARGE_OBJECT_SIZES ON)

# ORT ships .fbs.h files that were pre-generated by flatc 23.5.26 and
# static_assert exact equality against those version macros. ClickHouse
# bundles flatbuffers 24.x, so those committed headers are unusable here.
# Build flatc from the bundled flatbuffers sources and regenerate the .fbs.h
# files into the build directory, then put that directory first on the
# include path so ORT's own headers find the fresh copies.
set(_fb_src "${ClickHouse_SOURCE_DIR}/contrib/flatbuffers")

if (CMAKE_CROSSCOMPILING)
    message(FATAL_ERROR
        "ONNX Runtime requires regenerating flatbuffers schemas with flatc, "
        "which must run on the build host. Cross-compile support requires an "
        "externally-provided host flatc (not yet wired up).")
endif ()

# Minimal flatc build: the compiler-only source set from the flatbuffers
# CMakeLists (FlatBuffers_Compiler_SRCS) — all emitters plus the library core.
set(_flatc_srcs
    "${_fb_src}/src/idl_parser.cpp"
    "${_fb_src}/src/idl_gen_text.cpp"
    "${_fb_src}/src/reflection.cpp"
    "${_fb_src}/src/util.cpp"
    "${_fb_src}/src/idl_gen_binary.cpp"
    "${_fb_src}/src/idl_gen_cpp.cpp"
    "${_fb_src}/src/idl_gen_csharp.cpp"
    "${_fb_src}/src/idl_gen_dart.cpp"
    "${_fb_src}/src/idl_gen_kotlin.cpp"
    "${_fb_src}/src/idl_gen_kotlin_kmp.cpp"
    "${_fb_src}/src/idl_gen_go.cpp"
    "${_fb_src}/src/idl_gen_java.cpp"
    "${_fb_src}/src/idl_gen_ts.cpp"
    "${_fb_src}/src/idl_gen_php.cpp"
    "${_fb_src}/src/idl_gen_python.cpp"
    "${_fb_src}/src/idl_gen_lobster.cpp"
    "${_fb_src}/src/idl_gen_rust.cpp"
    "${_fb_src}/src/idl_gen_fbs.cpp"
    "${_fb_src}/src/idl_gen_grpc.cpp"
    "${_fb_src}/src/idl_gen_json_schema.cpp"
    "${_fb_src}/src/idl_gen_swift.cpp"
    "${_fb_src}/src/file_name_saving_file_manager.cpp"
    "${_fb_src}/src/file_binary_writer.cpp"
    "${_fb_src}/src/file_writer.cpp"
    "${_fb_src}/src/flatc.cpp"
    "${_fb_src}/src/flatc_main.cpp"
    "${_fb_src}/src/binary_annotator.cpp"
    "${_fb_src}/src/annotated_binary_text_gen.cpp"
    "${_fb_src}/src/bfbs_gen_lua.cpp"
    "${_fb_src}/src/bfbs_gen_nim.cpp"
    "${_fb_src}/src/code_generators.cpp"
    "${_fb_src}/grpc/src/compiler/cpp_generator.cc"
    "${_fb_src}/grpc/src/compiler/go_generator.cc"
    "${_fb_src}/grpc/src/compiler/java_generator.cc"
    "${_fb_src}/grpc/src/compiler/python_generator.cc"
    "${_fb_src}/grpc/src/compiler/swift_generator.cc"
    "${_fb_src}/grpc/src/compiler/ts_generator.cc"
)
add_executable(_ort_flatc ${_flatc_srcs})
target_include_directories(_ort_flatc PRIVATE
    "${_fb_src}/include"
    "${_fb_src}/grpc")
target_compile_definitions(_ort_flatc PRIVATE FLATBUFFERS_LOCALE_INDEPENDENT=0)
set_target_properties(_ort_flatc PROPERTIES
    CXX_STANDARD 17
    SKIP_CHECK_LARGE_OBJECT_SIZES ON)

# Regenerate each .fbs in place. ORT's .cc sources include the .fbs.h headers
# with double-quoted relative paths (e.g. #include "adapter_format/adapter_schema.fbs.h"),
# which the compiler resolves relative to the including file BEFORE consulting
# the include path — so a build-dir shadow is invisible and the stale copy
# committed to the submodule wins. Overwriting in place is what ORT's own
# build does; it marks the submodule working tree dirty, which is acceptable.
set(_ort_fbs_files
    "core/flatbuffers/schema/ort.fbs"
    "core/flatbuffers/schema/ort_training_checkpoint.fbs"
    "lora/adapter_format/adapter_schema.fbs"
)
set(_ort_fbs_generated_headers)
foreach (_fbs IN LISTS _ort_fbs_files)
    get_filename_component(_fbs_dir "${_fbs}" DIRECTORY)
    get_filename_component(_fbs_stem "${_fbs}" NAME_WE)
    set(_out_dir "${ORT_SRC}/onnxruntime/${_fbs_dir}")
    set(_out_h "${_out_dir}/${_fbs_stem}.fbs.h")
    add_custom_command(
        OUTPUT "${_out_h}"
        COMMAND $<TARGET_FILE:_ort_flatc>
            --cpp --scoped-enums --filename-suffix ".fbs"
            -o "${_out_dir}"
            "${ORT_SRC}/onnxruntime/${_fbs}"
        DEPENDS _ort_flatc "${ORT_SRC}/onnxruntime/${_fbs}"
        COMMENT "flatc: regenerating ${_fbs}.h against bundled flatbuffers"
        VERBATIM)
    list(APPEND _ort_fbs_generated_headers "${_out_h}")
endforeach ()
add_custom_target(_ort_fbs_gen DEPENDS ${_ort_fbs_generated_headers})
add_dependencies(_onnxruntime _ort_fbs_gen)

target_include_directories(_onnxruntime SYSTEM PRIVATE
    # include/onnxruntime/ is the root for #include "core/common/common.h" etc.
    "${ORT_SRC}/include/onnxruntime"
    "${ORT_SRC}/include/onnxruntime/core/session"
    "${ORT_SRC}/include"
    # onnxruntime/ for internal implementation files
    "${ORT_SRC}/onnxruntime"
    # Generated headers
    "${ORT_GEN_DIR}"
    # onnx schema
    "${ClickHouse_SOURCE_DIR}/contrib/onnx"
    # Header-only deps
    "${ClickHouse_SOURCE_DIR}/contrib/eigen"
    "${ClickHouse_SOURCE_DIR}/contrib/microsoft-gsl/include"
    "${ClickHouse_SOURCE_DIR}/contrib/boost"
    "${ClickHouse_SOURCE_DIR}/contrib/safeint"
    "${ClickHouse_SOURCE_DIR}/contrib/date/include"
    "${ClickHouse_SOURCE_DIR}/contrib/pytorch_cpuinfo/include"
    # MLAS internal headers
    "${ORT_SRC}/onnxruntime/core/mlas/inc"
    "${ORT_SRC}/onnxruntime/core/mlas/lib"
    # ClickHouse deps
    "${ClickHouse_SOURCE_DIR}/contrib/google-protobuf/src"
    "${ClickHouse_SOURCE_DIR}/contrib/abseil-cpp"
    "${ClickHouse_SOURCE_DIR}/contrib/flatbuffers/include"
    "${ClickHouse_SOURCE_DIR}/contrib/re2"
    "${ClickHouse_SOURCE_DIR}/contrib/nlohmann-json/include")

# MLAS ISA-specific files — each group requires its own target features
set_source_files_properties(
    "${ORT_SRC}/onnxruntime/core/mlas/lib/intrinsics/avx/min_max_elements.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/DgemmKernelAvx.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SconvKernelAvx.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SgemmKernelAvx.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SgemmKernelM1Avx.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SgemmKernelM1TransposeBAvx.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SgemmTransposePackB16x4Avx.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SoftmaxKernelAvx.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SpoolKernelAvx.S"
    PROPERTIES COMPILE_FLAGS "-mavx")

set_source_files_properties(
    "${ORT_SRC}/onnxruntime/core/mlas/lib/intrinsics/avx2/qdwconv_avx2.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/intrinsics/avx2/qladd_avx2.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/intrinsics/avx2/saturation_check_avx2.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/rotary_embedding_kernel_avx2.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/sqnbitgemm_kernel_avx2.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/sqnbitgemm_lut_kernel_avx2.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/halfgemm.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/ConvSymKernelAvx2.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/DgemmKernelFma3.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/ErfKernelFma3.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/LogisticKernelFma3.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/QgemmU8S8KernelAvx2.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/QgemmU8U8KernelAvx2.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/QgemmU8X8KernelAvx2.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/QgemvU8S8KernelAvx2.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/QgemvU8S8KernelAvxVnni.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SconvKernelFma3.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SgemmKernelFma3.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/TanhKernelFma3.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/TransKernelFma3.S"
    PROPERTIES COMPILE_FLAGS "-mavx2 -mavxvnni -mf16c -mfma")

set_source_files_properties(
    "${ORT_SRC}/onnxruntime/core/mlas/lib/intrinsics/avx512/gelu_avx512f.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/intrinsics/avx512/quantize_avx512f.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/intrinsics/avx512/silu_avx512f.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/DgemmKernelAvx512F.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SconvKernelAvx512F.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SgemmKernelAvx512F.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SoftmaxKernelAvx512F.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SpoolKernelAvx512F.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/TransKernelAvx512F.S"
    PROPERTIES COMPILE_FLAGS "-mavx512f")

set_source_files_properties(
    "${ORT_SRC}/onnxruntime/core/mlas/lib/sqnbitgemm_kernel_avx512.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/ConvSymKernelAvx512Core.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/QgemmU8X8KernelAvx512Core.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/QgemvU8S8KernelAvx512Core.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/QgemvU8S8KernelAvx512Vnni.S"
    PROPERTIES COMPILE_FLAGS "-mavx512bw -mavx512dq -mavx512vl -mavx512vnni -mfma")

set_source_files_properties(
    "${ORT_SRC}/onnxruntime/core/mlas/lib/q4gemm_avx512.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/sqnbitgemm_kernel_avx512vnni.cpp"
    PROPERTIES COMPILE_FLAGS "-mavx512bw -mavx512dq -mavx512f -mavx512vl -mavx512vnni -mfma")

set_source_files_properties(
    "${ORT_SRC}/onnxruntime/core/mlas/lib/qgemm_kernel_amx.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/QgemmU8S8KernelAmx.S"
    PROPERTIES COMPILE_FLAGS "-mavx2 -mavx512bw -mavx512dq -mavx512f -mavx512vl")

set_source_files_properties(
    "${ORT_SRC}/onnxruntime/core/mlas/lib/qgemm_kernel_sse.cpp"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/DgemmKernelSse2.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SconvKernelSse2.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SgemmKernelSse2.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SgemmTransposePackB16x4Sse2.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/SpoolKernelSse2.S"
    "${ORT_SRC}/onnxruntime/core/mlas/lib/x86_64/cvtfp16a.S"
    PROPERTIES COMPILE_FLAGS "-msse2")

set_source_files_properties(
    "${ORT_SRC}/onnxruntime/core/common/spin_pause.cc"
    PROPERTIES COMPILE_FLAGS "-mwaitpkg")



target_compile_definitions(_onnxruntime PRIVATE
    ONNX_ML=1
    ONNX_NAMESPACE=onnx
    ONNX_USE_LITE_PROTO=1
    ORT_NO_RTTI
    ORT_ENABLE_STREAM
    PLATFORM_POSIX
    CPUINFO_SUPPORTED
    CPUINFO_SUPPORTED_PLATFORM=1
    EIGEN_MPL2_ONLY
    EIGEN_USE_THREADS
    ONLY_C_LOCALE=0
    __ONNX_NO_DOC_STRINGS
    _GNU_SOURCE)

target_link_libraries(_onnxruntime
    PRIVATE
        ch_contrib::protobuf
        ch_contrib::abseil_swiss_tables  # provides absl::* targets
        ch_contrib::re2
        _ort_onnx
        _ort_cpuinfo
        pthread dl)
# flatbuffers is header-only for reading — already in include dirs above

add_library(ch_contrib::onnxruntime ALIAS _onnxruntime)
set(ONNXRUNTIME_INCLUDE_PATH
    "${ORT_SRC}/include/onnxruntime/core/session"
    CACHE INTERNAL "ORT include path")

if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options(_onnxruntime PRIVATE "-g0")
    target_compile_options(_ort_onnx PRIVATE "-g0")
    target_compile_options(_ort_cpuinfo PRIVATE "-g0")
endif()

message(STATUS "ONNX Runtime: source build")
