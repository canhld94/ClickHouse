
#include <stdint.h>

extern uint64_t clickhouse_server_version();
extern void clickhouse_log(const char * message, uint32_t length);
extern void clickhouse_throw(const char * message, uint32_t length);
extern void clickhouse_random(void * data, uint32_t size);

uint32_t int_to_str(uint64_t n, char * buf) {
    uint32_t len = 0;
    uint64_t t = n;
    while (t > 0) {
        t /= 10;
        len++;
    }
    for (uint32_t i = 0; i < len; i++) {
        buf[len - i - 1] = '0' + n % 10;
        n /= 10;
    }
    return len;
}


void copy_str(const char * src, char * dst, uint32_t len) {
    for (uint32_t i = 0; i < len; i++) {
        dst[i] = src[i];
    }
}

uint32_t test_func(uint32_t terminate) {
    uint64_t version = clickhouse_server_version();

    char buf[64];
    copy_str("Hello, ClickHouse ", buf, 18);
    uint32_t len = int_to_str(version, buf + 18);
    copy_str("!", buf + 18 + len, 1);
    clickhouse_log(buf, 19 + len);

    if (terminate) {
        clickhouse_throw("Goodbye, ClickHouse!", 20);
    }
    return 0;
}

uint32_t test_random(uint32_t) {
    uint8_t buf[16];
    clickhouse_random(buf, sizeof(buf));
    uint32_t sum = 0;
    for (uint32_t i = 0; i < sizeof(buf); i++) {
        sum += buf[i];
    }
    return sum;
}
