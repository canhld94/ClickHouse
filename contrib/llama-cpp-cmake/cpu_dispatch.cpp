/// Runtime CPU dispatch for llama.cpp / ggml.
///
/// We compile the ggml-cpu backend three times (sse42, avx2, avx512) with
/// localized symbols.  This file detects the CPU and returns the best variant
/// when ggml-backend-reg.cpp calls ggml_backend_cpu_reg().

#include <ggml-backend.h>
#include <Common/CPUID.h>

// Variant entry points (each lives in its own localized .o)
extern "C" {
ggml_backend_reg_t ggml_backend_cpu_reg_sse42(void);
ggml_backend_reg_t ggml_backend_cpu_reg_avx2(void);
ggml_backend_reg_t ggml_backend_cpu_reg_avx512(void);
}

static const char * selected_variant_name = "unknown";

extern "C" const char * ggml_cpu_variant_name(void)
{
    return selected_variant_name;
}

extern "C" ggml_backend_reg_t ggml_backend_cpu_reg(void)
{
    static ggml_backend_reg_t selected = nullptr;
    if (selected)
        return selected;

#if defined(__x86_64__)
    if (DB::CPU::CPUFlagsCache::have_AVX512F && DB::CPU::CPUFlagsCache::have_AVX512BW && DB::CPU::CPUFlagsCache::have_AVX512VL)
    {
        selected = ggml_backend_cpu_reg_avx512();
        selected_variant_name = "avx512";
    }
    else if (DB::CPU::CPUFlagsCache::have_AVX2 && DB::CPU::CPUFlagsCache::have_FMA)
    {
        selected = ggml_backend_cpu_reg_avx2();
        selected_variant_name = "avx2";
    }
    else
    {
        selected = ggml_backend_cpu_reg_sse42();
        selected_variant_name = "sse42";
    }
#else
    selected = ggml_backend_cpu_reg_sse42();
    selected_variant_name = "sse42";
#endif

    return selected;
}
