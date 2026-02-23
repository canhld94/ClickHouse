#include <stdint.h>

#define DATA_SIZE (1024 * 1024)

static int32_t large_data[DATA_SIZE] = { 42 };

int32_t access_data(uint32_t index) {
    if (index < DATA_SIZE) {
        return large_data[index];
    }
    return -1;
}
