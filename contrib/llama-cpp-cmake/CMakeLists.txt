option(ENABLE_LLAMACPP "Enable LlamaCPP (required for embedding functions)" ${ENABLE_LIBRARIES})

if (NOT ENABLE_LLAMACPP)
    message(STATUS "Not using LlamaCPP")
    return()
endif()


set(LLAMA_SRC "${ClickHouse_SOURCE_DIR}/contrib/llama.cpp")

if (NOT LLAMA_CPP_COMMIT)
    set(LLAMA_CPP_COMMIT "unknown")
endif()

# ---------------------------------------------------------------------------
# Source file lists
# ---------------------------------------------------------------------------

# CPU-variant sources: SIMD-hot code compiled once per variant (sse42, avx2, avx512)
set(LLAMA_CPU_VARIANT_SRCS
    "${LLAMA_SRC}/ggml/src/ggml-cpu/ggml-cpu.c"
    "${LLAMA_SRC}/ggml/src/ggml-cpu/ggml-cpu.cpp"
    "${LLAMA_SRC}/ggml/src/ggml-cpu/ops.cpp"
    "${LLAMA_SRC}/ggml/src/ggml-cpu/vec.cpp"
    "${LLAMA_SRC}/ggml/src/ggml-cpu/quants.c"
    "${LLAMA_SRC}/ggml/src/ggml-cpu/repack.cpp"
    "${LLAMA_SRC}/ggml/src/ggml-cpu/binary-ops.cpp"
    "${LLAMA_SRC}/ggml/src/ggml-cpu/unary-ops.cpp"
    "${LLAMA_SRC}/ggml/src/ggml-cpu/traits.cpp"
    "${LLAMA_SRC}/ggml/src/ggml-cpu/hbm.cpp"
    "${LLAMA_SRC}/ggml/src/ggml-cpu/amx/amx.cpp"
    "${LLAMA_SRC}/ggml/src/ggml-cpu/amx/mmq.cpp"
    "${LLAMA_SRC}/ggml/src/ggml-cpu/llamafile/sgemm.cpp"
)

# arch/x86 files require at least AVX2 — only included in avx2+ variants
set(LLAMA_CPU_X86_AVX2_SRCS
    "${LLAMA_SRC}/ggml/src/ggml-cpu/arch/x86/quants.c"
    "${LLAMA_SRC}/ggml/src/ggml-cpu/arch/x86/repack.cpp"
)

# Common sources: compiled once, no SIMD dependency
set(LLAMA_COMMON_SRCS
    "${LLAMA_SRC}/ggml/src/ggml.c"
    "${LLAMA_SRC}/ggml/src/ggml.cpp"
    "${LLAMA_SRC}/ggml/src/ggml-alloc.c"
    "${LLAMA_SRC}/ggml/src/ggml-backend.cpp"
    "${LLAMA_SRC}/ggml/src/ggml-backend-meta.cpp"
    "${LLAMA_SRC}/ggml/src/ggml-backend-reg.cpp"
    "${LLAMA_SRC}/ggml/src/ggml-backend-dl.cpp"
    "${LLAMA_SRC}/ggml/src/ggml-opt.cpp"
    "${LLAMA_SRC}/ggml/src/ggml-quants.c"
    "${LLAMA_SRC}/ggml/src/ggml-threading.cpp"
    "${LLAMA_SRC}/ggml/src/gguf.cpp"
    "${LLAMA_SRC}/src/llama-adapter.cpp"
    "${LLAMA_SRC}/src/llama-arch.cpp"
    "${LLAMA_SRC}/src/llama-batch.cpp"
    "${LLAMA_SRC}/src/llama-chat.cpp"
    "${LLAMA_SRC}/src/llama-context.cpp"
    "${LLAMA_SRC}/src/llama-cparams.cpp"
    "${LLAMA_SRC}/src/llama-grammar.cpp"
    "${LLAMA_SRC}/src/llama-graph.cpp"
    "${LLAMA_SRC}/src/llama-hparams.cpp"
    "${LLAMA_SRC}/src/llama-impl.cpp"
    "${LLAMA_SRC}/src/llama-io.cpp"
    "${LLAMA_SRC}/src/llama-kv-cache-iswa.cpp"
    "${LLAMA_SRC}/src/llama-kv-cache.cpp"
    "${LLAMA_SRC}/src/llama-memory-hybrid-iswa.cpp"
    "${LLAMA_SRC}/src/llama-memory-hybrid.cpp"
    "${LLAMA_SRC}/src/llama-memory-recurrent.cpp"
    "${LLAMA_SRC}/src/llama-memory.cpp"
    "${LLAMA_SRC}/src/llama-mmap.cpp"
    "${LLAMA_SRC}/src/llama-model-loader.cpp"
    "${LLAMA_SRC}/src/llama-model-saver.cpp"
    "${LLAMA_SRC}/src/llama-model.cpp"
    "${LLAMA_SRC}/src/llama-quant.cpp"
    "${LLAMA_SRC}/src/llama-sampler.cpp"
    "${LLAMA_SRC}/src/llama-vocab.cpp"
    "${LLAMA_SRC}/src/llama.cpp"
    "${LLAMA_SRC}/src/models/afmoe.cpp"
    "${LLAMA_SRC}/src/models/apertus.cpp"
    "${LLAMA_SRC}/src/models/arcee.cpp"
    "${LLAMA_SRC}/src/models/arctic.cpp"
    "${LLAMA_SRC}/src/models/arwkv7.cpp"
    "${LLAMA_SRC}/src/models/baichuan.cpp"
    "${LLAMA_SRC}/src/models/bailingmoe.cpp"
    "${LLAMA_SRC}/src/models/bailingmoe2.cpp"
    "${LLAMA_SRC}/src/models/bert.cpp"
    "${LLAMA_SRC}/src/models/bitnet.cpp"
    "${LLAMA_SRC}/src/models/bloom.cpp"
    "${LLAMA_SRC}/src/models/chameleon.cpp"
    "${LLAMA_SRC}/src/models/chatglm.cpp"
    "${LLAMA_SRC}/src/models/codeshell.cpp"
    "${LLAMA_SRC}/src/models/cogvlm.cpp"
    "${LLAMA_SRC}/src/models/cohere2-iswa.cpp"
    "${LLAMA_SRC}/src/models/command-r.cpp"
    "${LLAMA_SRC}/src/models/dbrx.cpp"
    "${LLAMA_SRC}/src/models/deci.cpp"
    "${LLAMA_SRC}/src/models/deepseek.cpp"
    "${LLAMA_SRC}/src/models/deepseek2.cpp"
    "${LLAMA_SRC}/src/models/delta-net-base.cpp"
    "${LLAMA_SRC}/src/models/dots1.cpp"
    "${LLAMA_SRC}/src/models/dream.cpp"
    "${LLAMA_SRC}/src/models/ernie4-5-moe.cpp"
    "${LLAMA_SRC}/src/models/ernie4-5.cpp"
    "${LLAMA_SRC}/src/models/eurobert.cpp"
    "${LLAMA_SRC}/src/models/exaone-moe.cpp"
    "${LLAMA_SRC}/src/models/exaone.cpp"
    "${LLAMA_SRC}/src/models/exaone4.cpp"
    "${LLAMA_SRC}/src/models/falcon-h1.cpp"
    "${LLAMA_SRC}/src/models/falcon.cpp"
    "${LLAMA_SRC}/src/models/gemma-embedding.cpp"
    "${LLAMA_SRC}/src/models/gemma.cpp"
    "${LLAMA_SRC}/src/models/gemma2-iswa.cpp"
    "${LLAMA_SRC}/src/models/gemma3.cpp"
    "${LLAMA_SRC}/src/models/gemma3n-iswa.cpp"
    "${LLAMA_SRC}/src/models/gemma4-iswa.cpp"
    "${LLAMA_SRC}/src/models/glm4-moe.cpp"
    "${LLAMA_SRC}/src/models/glm4.cpp"
    "${LLAMA_SRC}/src/models/gpt2.cpp"
    "${LLAMA_SRC}/src/models/gptneox.cpp"
    "${LLAMA_SRC}/src/models/granite-hybrid.cpp"
    "${LLAMA_SRC}/src/models/granite.cpp"
    "${LLAMA_SRC}/src/models/grok.cpp"
    "${LLAMA_SRC}/src/models/grovemoe.cpp"
    "${LLAMA_SRC}/src/models/hunyuan-dense.cpp"
    "${LLAMA_SRC}/src/models/hunyuan-moe.cpp"
    "${LLAMA_SRC}/src/models/internlm2.cpp"
    "${LLAMA_SRC}/src/models/jais.cpp"
    "${LLAMA_SRC}/src/models/jais2.cpp"
    "${LLAMA_SRC}/src/models/jamba.cpp"
    "${LLAMA_SRC}/src/models/kimi-linear.cpp"
    "${LLAMA_SRC}/src/models/lfm2.cpp"
    "${LLAMA_SRC}/src/models/llada-moe.cpp"
    "${LLAMA_SRC}/src/models/llada.cpp"
    "${LLAMA_SRC}/src/models/llama-iswa.cpp"
    "${LLAMA_SRC}/src/models/llama.cpp"
    "${LLAMA_SRC}/src/models/maincoder.cpp"
    "${LLAMA_SRC}/src/models/mamba-base.cpp"
    "${LLAMA_SRC}/src/models/mamba.cpp"
    "${LLAMA_SRC}/src/models/mimo2-iswa.cpp"
    "${LLAMA_SRC}/src/models/minicpm3.cpp"
    "${LLAMA_SRC}/src/models/minimax-m2.cpp"
    "${LLAMA_SRC}/src/models/mistral3.cpp"
    "${LLAMA_SRC}/src/models/modern-bert.cpp"
    "${LLAMA_SRC}/src/models/mpt.cpp"
    "${LLAMA_SRC}/src/models/nemotron-h.cpp"
    "${LLAMA_SRC}/src/models/nemotron.cpp"
    "${LLAMA_SRC}/src/models/neo-bert.cpp"
    "${LLAMA_SRC}/src/models/olmo.cpp"
    "${LLAMA_SRC}/src/models/olmo2.cpp"
    "${LLAMA_SRC}/src/models/olmoe.cpp"
    "${LLAMA_SRC}/src/models/openai-moe-iswa.cpp"
    "${LLAMA_SRC}/src/models/openelm.cpp"
    "${LLAMA_SRC}/src/models/orion.cpp"
    "${LLAMA_SRC}/src/models/paddleocr.cpp"
    "${LLAMA_SRC}/src/models/pangu-embedded.cpp"
    "${LLAMA_SRC}/src/models/phi2.cpp"
    "${LLAMA_SRC}/src/models/phi3.cpp"
    "${LLAMA_SRC}/src/models/plamo.cpp"
    "${LLAMA_SRC}/src/models/plamo2.cpp"
    "${LLAMA_SRC}/src/models/plamo3.cpp"
    "${LLAMA_SRC}/src/models/plm.cpp"
    "${LLAMA_SRC}/src/models/qwen.cpp"
    "${LLAMA_SRC}/src/models/qwen2.cpp"
    "${LLAMA_SRC}/src/models/qwen2moe.cpp"
    "${LLAMA_SRC}/src/models/qwen2vl.cpp"
    "${LLAMA_SRC}/src/models/qwen3.cpp"
    "${LLAMA_SRC}/src/models/qwen35.cpp"
    "${LLAMA_SRC}/src/models/qwen35moe.cpp"
    "${LLAMA_SRC}/src/models/qwen3moe.cpp"
    "${LLAMA_SRC}/src/models/qwen3next.cpp"
    "${LLAMA_SRC}/src/models/qwen3vl-moe.cpp"
    "${LLAMA_SRC}/src/models/qwen3vl.cpp"
    "${LLAMA_SRC}/src/models/refact.cpp"
    "${LLAMA_SRC}/src/models/rnd1.cpp"
    "${LLAMA_SRC}/src/models/rwkv6-base.cpp"
    "${LLAMA_SRC}/src/models/rwkv6.cpp"
    "${LLAMA_SRC}/src/models/rwkv6qwen2.cpp"
    "${LLAMA_SRC}/src/models/rwkv7-base.cpp"
    "${LLAMA_SRC}/src/models/rwkv7.cpp"
    "${LLAMA_SRC}/src/models/seed-oss.cpp"
    "${LLAMA_SRC}/src/models/smallthinker.cpp"
    "${LLAMA_SRC}/src/models/smollm3.cpp"
    "${LLAMA_SRC}/src/models/stablelm.cpp"
    "${LLAMA_SRC}/src/models/starcoder.cpp"
    "${LLAMA_SRC}/src/models/starcoder2.cpp"
    "${LLAMA_SRC}/src/models/step35-iswa.cpp"
    "${LLAMA_SRC}/src/models/t5-dec.cpp"
    "${LLAMA_SRC}/src/models/t5-enc.cpp"
    "${LLAMA_SRC}/src/models/wavtokenizer-dec.cpp"
    "${LLAMA_SRC}/src/models/xverse.cpp"
    "${LLAMA_SRC}/src/unicode-data.cpp"
    "${LLAMA_SRC}/src/unicode.cpp"
)

# ---------------------------------------------------------------------------
# CPU variant: compile SIMD-hot code with different flags, localize symbols
# ---------------------------------------------------------------------------

set(LLAMA_CPU_COMMON_DEFS
    GGML_USE_CPU
    GGML_USE_LLAMAFILE
    NDEBUG
    _GNU_SOURCE
    "GGML_VERSION=\"0.9.11\""
    "GGML_COMMIT=\"${LLAMA_CPP_COMMIT}\""
)

set(LLAMA_CPU_VARIANT_OBJS "")

function(add_llama_cpu_variant VARIANT_NAME)
    set(multiValueArgs FLAGS EXTRA_SRCS EXTRA_DEFS)
    cmake_parse_arguments(ARG "" "" "${multiValueArgs}" ${ARGN})

    # Generate a thin wrapper that exports the registration function under a unique name.
    # The wrapper is compiled together with the variant, so ld -r resolves the internal ref.
    # After objcopy --localize-hidden, only our wrapper symbol stays global.
    set(WRAPPER "${CMAKE_CURRENT_BINARY_DIR}/cpu_variant_${VARIANT_NAME}.cpp")
    file(WRITE "${WRAPPER}" "\
#include <ggml-backend.h>\n\
extern \"C\" ggml_backend_reg_t ggml_backend_cpu_reg(void);\n\
extern \"C\" __attribute__((visibility(\"default\")))\n\
ggml_backend_reg_t ggml_backend_cpu_reg_${VARIANT_NAME}(void) {\n\
    return ggml_backend_cpu_reg();\n\
}\n")

    set(OBJ_TARGET _llama_cpu_${VARIANT_NAME})

    add_library(${OBJ_TARGET} OBJECT ${LLAMA_CPU_VARIANT_SRCS} ${ARG_EXTRA_SRCS} "${WRAPPER}")
    target_compile_options(${OBJ_TARGET} PRIVATE -w -fvisibility=hidden ${ARG_FLAGS})
    target_compile_definitions(${OBJ_TARGET} PRIVATE ${LLAMA_CPU_COMMON_DEFS} ${ARG_EXTRA_DEFS})
    target_include_directories(${OBJ_TARGET} SYSTEM PUBLIC
        "${LLAMA_SRC}/include"
        "${LLAMA_SRC}/ggml/include"
    )
    target_include_directories(${OBJ_TARGET} SYSTEM PRIVATE
        "${LLAMA_SRC}/src"
        "${LLAMA_SRC}/ggml/src"
        "${LLAMA_SRC}/ggml/src/ggml-cpu"
    )

    if (OMIT_HEAVY_DEBUG_SYMBOLS)
        target_compile_options(${OBJ_TARGET} PRIVATE "-g0")
    endif()

    # Partial-link all objects into a single relocatable, then localize hidden symbols.
    # After this, only ggml_backend_cpu_reg_<variant>() is globally visible.
    set(MERGED_OBJ "${CMAKE_CURRENT_BINARY_DIR}/llama_cpu_${VARIANT_NAME}.o")

    # Write object file list to a response file (TARGET_OBJECTS uses semicolons)
    set(RSP_FILE "${CMAKE_CURRENT_BINARY_DIR}/llama_cpu_${VARIANT_NAME}.rsp")
    file(GENERATE OUTPUT "${RSP_FILE}" CONTENT "$<JOIN:$<TARGET_OBJECTS:${OBJ_TARGET}>,\n>")

    add_custom_command(
        OUTPUT "${MERGED_OBJ}"
        COMMAND ${CMAKE_LINKER} -r -o "${MERGED_OBJ}" @"${RSP_FILE}"
        COMMAND ${CMAKE_OBJCOPY} --localize-hidden "${MERGED_OBJ}"
        DEPENDS ${OBJ_TARGET} "${RSP_FILE}"
        COMMENT "llama.cpp: building CPU variant ${VARIANT_NAME}"
    )

    set(LLAMA_CPU_VARIANT_OBJS ${LLAMA_CPU_VARIANT_OBJS} "${MERGED_OBJ}" PARENT_SCOPE)
endfunction()

# Three variants: baseline SSE4.2, AVX2, AVX-512
# arch/x86 quants/repack require AVX2+, so only added to avx2 and avx512 variants
add_llama_cpu_variant(sse42  FLAGS -msse4.2 EXTRA_DEFS GGML_CPU_GENERIC)
add_llama_cpu_variant(avx2   FLAGS -mavx -mavx2 -mfma -mf16c -mbmi2
                              EXTRA_SRCS ${LLAMA_CPU_X86_AVX2_SRCS})
add_llama_cpu_variant(avx512 FLAGS -mavx -mavx2 -mfma -mf16c -mbmi2
                                   -mavx512f -mavx512cd -mavx512vl -mavx512dq -mavx512bw
                                   -mavx512vbmi -mavx512vnni -mavx512bf16
                              EXTRA_SRCS ${LLAMA_CPU_X86_AVX2_SRCS})

# clang-20 spends >10 min in SelectionDAG on this file at -O3 with AVX-512 flags
# (DAGCombiner + SimplifyDemandedBits / computeKnownBits recursion on the huge
# DAG from the hand-vectorized intrinsics). The intrinsics are already tuned —
# -O0 skips the heavy mid-end passes and the file compiles in seconds without
# giving up meaningful codegen quality for this one translation unit.
set_source_files_properties(
    "${LLAMA_SRC}/ggml/src/ggml-cpu/arch/x86/repack.cpp"
    TARGET_DIRECTORY _llama_cpu_avx512
    PROPERTIES COMPILE_OPTIONS "-O0")

# ---------------------------------------------------------------------------
# Final library: common code + dispatcher + variant objects
# ---------------------------------------------------------------------------

add_library(_llama_cpp STATIC
    ${LLAMA_COMMON_SRCS}
    "${CMAKE_CURRENT_SOURCE_DIR}/cpu_dispatch.cpp"
)

# The variant .o files are linked into the static library
target_sources(_llama_cpp PRIVATE ${LLAMA_CPU_VARIANT_OBJS})
set_source_files_properties(${LLAMA_CPU_VARIANT_OBJS} PROPERTIES EXTERNAL_OBJECT TRUE GENERATED TRUE)

target_include_directories(_llama_cpp SYSTEM PUBLIC
    "${LLAMA_SRC}/include"
    "${LLAMA_SRC}/ggml/include"
)

target_include_directories(_llama_cpp SYSTEM PRIVATE
    "${LLAMA_SRC}/src"
    "${LLAMA_SRC}/ggml/src"
    "${LLAMA_SRC}/ggml/src/ggml-cpu"
)

# cpu_dispatch.cpp uses Common/CPUID.h for runtime feature detection
target_include_directories(_llama_cpp PRIVATE
    "${ClickHouse_SOURCE_DIR}/src"
    "${ClickHouse_SOURCE_DIR}/base"
)

# GGML_USE_CPU is defined so ggml-backend-reg.cpp calls ggml_backend_cpu_reg() at init.
# Our cpu_dispatch.cpp provides that symbol and routes to the best variant.
target_compile_definitions(_llama_cpp PRIVATE
    GGML_USE_CPU
    NDEBUG
    _GNU_SOURCE
    "GGML_VERSION=\"0.9.11\""
    "GGML_COMMIT=\"${LLAMA_CPP_COMMIT}\""
)

target_compile_options(_llama_cpp PRIVATE -w)

if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options(_llama_cpp PRIVATE "-g0")
endif()

add_library(ch_contrib::llama_cpp ALIAS _llama_cpp)
