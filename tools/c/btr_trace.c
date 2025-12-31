#include <assert.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "innodb.h"
#include "test0aux.h"

#define DATABASE "trace_db"
#define TABLE "trace_t"

static ib_err_t create_database(const char *name) {
    ib_bool_t ok = ib_database_create(name);
    assert(ok == IB_TRUE);
    return DB_SUCCESS;
}

static ib_err_t create_table(const char *dbname, const char *name) {
    ib_trx_t ib_trx;
    ib_id_t table_id = 0;
    ib_err_t err = DB_SUCCESS;
    ib_tbl_sch_t ib_tbl_sch = NULL;
    ib_idx_sch_t ib_idx_sch = NULL;
    char table_name[IB_MAX_TABLE_NAME_LEN];

#ifdef __WIN__
    sprintf(table_name, "%s/%s", dbname, name);
#else
    snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
#endif

    err = ib_table_schema_create(table_name, &ib_tbl_sch, IB_TBL_COMPACT, 0);
    assert(err == DB_SUCCESS);

    err = ib_table_schema_add_col(ib_tbl_sch, "c1", IB_INT, IB_COL_NONE, 0, sizeof(int));
    assert(err == DB_SUCCESS);

    err = ib_table_schema_add_index(ib_tbl_sch, "PRIMARY", &ib_idx_sch);
    assert(err == DB_SUCCESS);

    err = ib_index_schema_add_col(ib_idx_sch, "c1", 0);
    assert(err == DB_SUCCESS);

    err = ib_index_schema_set_clustered(ib_idx_sch);
    assert(err == DB_SUCCESS);

    ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
    assert(ib_trx != NULL);

    err = ib_schema_lock_exclusive(ib_trx);
    assert(err == DB_SUCCESS);

    err = ib_table_create(ib_trx, ib_tbl_sch, &table_id);
    assert(err == DB_SUCCESS);

    err = ib_trx_commit(ib_trx);
    assert(err == DB_SUCCESS);

    if (ib_tbl_sch != NULL) {
        ib_table_schema_delete(ib_tbl_sch);
    }

    return err;
}

static ib_err_t open_table(const char *dbname, const char *name, ib_trx_t ib_trx, ib_crsr_t *crsr) {
    ib_err_t err = DB_SUCCESS;
    char table_name[IB_MAX_TABLE_NAME_LEN];

#ifdef __WIN__
    sprintf(table_name, "%s/%s", dbname, name);
#else
    snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);
#endif

    err = ib_cursor_open_table(table_name, ib_trx, crsr);
    assert(err == DB_SUCCESS);

    return err;
}

typedef struct {
    uint64_t s[4];
} xoshiro256;

static uint64_t splitmix64_next(uint64_t *state) {
    uint64_t z = (*state += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

static uint64_t rotl64(uint64_t x, int k) {
    return (x << k) | (x >> (64 - k));
}

static void xoshiro_seed(xoshiro256 *rng, uint64_t seed) {
    uint64_t sm = seed;
    rng->s[0] = splitmix64_next(&sm);
    rng->s[1] = splitmix64_next(&sm);
    rng->s[2] = splitmix64_next(&sm);
    rng->s[3] = splitmix64_next(&sm);
}

static uint64_t xoshiro_next(xoshiro256 *rng) {
    const uint64_t r = rotl64(rng->s[0] + rng->s[3], 23) + rng->s[0];
    const uint64_t t = rng->s[1] << 17;

    rng->s[2] ^= rng->s[0];
    rng->s[3] ^= rng->s[1];
    rng->s[1] ^= rng->s[2];
    rng->s[0] ^= rng->s[3];

    rng->s[2] ^= t;
    rng->s[3] = rotl64(rng->s[3], 45);

    return r;
}

static uint64_t rand_u64(xoshiro256 *rng) {
    return xoshiro_next(rng);
}

static uint8_t rand_u8(xoshiro256 *rng) {
    return (uint8_t)xoshiro_next(rng);
}

static uint8_t rand_bool(xoshiro256 *rng) {
    return (uint8_t)(rand_u8(rng) & 1);
}

static uint8_t uint_less_than_u8(xoshiro256 *rng, uint8_t less_than) {
    uint8_t x = rand_u8(rng);
    uint16_t m = (uint16_t)x * (uint16_t)less_than;
    uint8_t l = (uint8_t)m;

    if (l < less_than) {
        uint8_t t = (uint8_t)(0 - less_than);
        if (t >= less_than) {
            t = (uint8_t)(t - less_than);
            if (t >= less_than) {
                t = (uint8_t)(t % less_than);
            }
        }
        while (l < t) {
            x = rand_u8(rng);
            m = (uint16_t)x * (uint16_t)less_than;
            l = (uint8_t)m;
        }
    }

    return (uint8_t)(m >> 8);
}

static uint64_t uint_less_than_u64(xoshiro256 *rng, uint64_t less_than) {
    uint64_t x = rand_u64(rng);
    __uint128_t m = (__uint128_t)x * less_than;
    uint64_t l = (uint64_t)m;

    if (l < less_than) {
        uint64_t t = (uint64_t)(0 - less_than);
        if (t >= less_than) {
            t -= less_than;
            if (t >= less_than) {
                t %= less_than;
            }
        }
        while (l < t) {
            x = rand_u64(rng);
            m = (__uint128_t)x * less_than;
            l = (uint64_t)m;
        }
    }

    return (uint64_t)(m >> 64);
}

static uint64_t uint_at_most_u64(xoshiro256 *rng, uint64_t at_most) {
    if (at_most == UINT64_MAX) {
        return rand_u64(rng);
    }
    return uint_less_than_u64(rng, at_most + 1);
}

static int64_t int_range_at_most_i64(xoshiro256 *rng, int64_t at_least, int64_t at_most) {
    uint64_t lo = (uint64_t)at_least;
    uint64_t hi = (uint64_t)at_most;
    uint64_t diff = hi - lo;
    uint64_t v = uint_at_most_u64(rng, diff);
    return (int64_t)(lo + v);
}

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s [options]\n"
            "  --seed <u64>   RNG seed (default 0xC0FFEE)\n"
            "  --ops <u64>    Operation count (default 60)\n"
            "  --help         Show this help\n",
            prog);
}

int main(int argc, char **argv) {
    uint64_t seed = 0xC0FFEEULL;
    uint64_t ops = 60;

    for (int i = 1; i < argc; ++i) {
        const char *arg = argv[i];
        if (strcmp(arg, "--help") == 0) {
            usage(argv[0]);
            return 0;
        }
        if (strcmp(arg, "--seed") == 0) {
            if (i + 1 >= argc) {
                usage(argv[0]);
                return 1;
            }
            seed = strtoull(argv[++i], NULL, 0);
            continue;
        }
        if (strcmp(arg, "--ops") == 0) {
            if (i + 1 >= argc) {
                usage(argv[0]);
                return 1;
            }
            ops = strtoull(argv[++i], NULL, 0);
            continue;
        }
        if (strncmp(arg, "--seed=", 7) == 0) {
            seed = strtoull(arg + 7, NULL, 0);
            continue;
        }
        if (strncmp(arg, "--ops=", 6) == 0) {
            ops = strtoull(arg + 6, NULL, 0);
            continue;
        }

        fprintf(stderr, "Unknown argument: %s\n", arg);
        usage(argv[0]);
        return 1;
    }

    printf("seed=%" PRIu64 " ops=%" PRIu64 "\n", seed, ops);

    xoshiro256 rng;
    xoshiro_seed(&rng, seed);

    OK(ib_init());
    test_configure();
    OK(ib_startup("barracuda"));

    OK(create_database(DATABASE));
    OK(create_table(DATABASE, TABLE));

    ib_trx_t ib_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
    assert(ib_trx != NULL);

    ib_crsr_t crsr;
    OK(open_table(DATABASE, TABLE, ib_trx, &crsr));
    OK(ib_cursor_lock(crsr, IB_LOCK_IX));

    ib_tpl_t ins_tpl = ib_clust_read_tuple_create(crsr);
    ib_tpl_t key_tpl = ib_clust_search_tuple_create(crsr);
    assert(ins_tpl != NULL);
    assert(key_tpl != NULL);

    int64_t *keys = (int64_t *)calloc(ops, sizeof(int64_t));
    uint8_t present[1001];
    size_t key_count = 0;
    memset(present, 0, sizeof(present));

    for (uint64_t op_idx = 0; op_idx < ops; ++op_idx) {
        uint8_t action = 0;
        if (key_count == 0) {
            action = 0;
        } else {
            action = uint_less_than_u8(&rng, 3);
        }

        if (action == 0) {
            int64_t key = int_range_at_most_i64(&rng, 1, 1000);
            size_t tries = 0;
            while (present[key] && tries < 10) {
                key = int_range_at_most_i64(&rng, 1, 1000);
                tries++;
            }
            if (present[key]) {
                continue;
            }

            OK(ib_tuple_write_i32(ins_tpl, 0, (int)key));
            ib_err_t err = ib_cursor_insert_row(crsr, ins_tpl);
            if (err != DB_SUCCESS) {
                fprintf(stderr, "Insert failed for key %" PRId64 ": %s\n", key, ib_strerror(err));
                return 1;
            }
            ins_tpl = ib_tuple_clear(ins_tpl);
            assert(ins_tpl != NULL);

            present[key] = 1;
            keys[key_count++] = key;
            printf("I %" PRId64 "\n", key);
        } else if (action == 1) {
            size_t idx = (size_t)uint_less_than_u64(&rng, (uint64_t)key_count);
            int64_t key = keys[idx];
            int res = 0;

            OK(ib_tuple_write_i32(key_tpl, 0, (int)key));
            ib_cursor_set_match_mode(crsr, IB_CLOSEST_MATCH);
            ib_err_t err = ib_cursor_moveto(crsr, key_tpl, IB_CUR_GE, &res);
            if (err != DB_SUCCESS || res != 0) {
                fprintf(stderr, "Delete lookup failed for key %" PRId64 "\n", key);
                return 1;
            }

            err = ib_cursor_delete_row(crsr);
            if (err != DB_SUCCESS) {
                fprintf(stderr, "Delete failed for key %" PRId64 ": %s\n", key, ib_strerror(err));
                return 1;
            }

            key_tpl = ib_tuple_clear(key_tpl);
            assert(key_tpl != NULL);

            present[key] = 0;
            keys[idx] = keys[key_count - 1];
            key_count--;
            printf("D %" PRId64 "\n", key);
        } else {
            int res = 0;
            int64_t search_key = int_range_at_most_i64(&rng, 1, 1000);

            if (key_count > 0 && rand_bool(&rng)) {
                size_t idx = (size_t)uint_less_than_u64(&rng, (uint64_t)key_count);
                search_key = keys[idx];
            }

            OK(ib_tuple_write_i32(key_tpl, 0, (int)search_key));
            ib_cursor_set_match_mode(crsr, IB_CLOSEST_MATCH);
            ib_err_t err = ib_cursor_moveto(crsr, key_tpl, IB_CUR_GE, &res);
            if (err != DB_SUCCESS && err != DB_END_OF_INDEX) {
                fprintf(stderr, "Search failed for key %" PRId64 ": %s\n", search_key, ib_strerror(err));
                return 1;
            }

            key_tpl = ib_tuple_clear(key_tpl);
            assert(key_tpl != NULL);

            printf("S %" PRId64 " %d\n", search_key, (err == DB_SUCCESS && res == 0) ? 1 : 0);
        }
    }

    if (ins_tpl != NULL) {
        ib_tuple_delete(ins_tpl);
    }
    if (key_tpl != NULL) {
        ib_tuple_delete(key_tpl);
    }

    OK(ib_cursor_close(crsr));
    OK(ib_trx_commit(ib_trx));

    ib_trx_t scan_trx = ib_trx_begin(IB_TRX_REPEATABLE_READ);
    assert(scan_trx != NULL);

    ib_crsr_t scan_crsr;
    OK(open_table(DATABASE, TABLE, scan_trx, &scan_crsr));
    OK(ib_cursor_lock(scan_crsr, IB_LOCK_IS));

    ib_tpl_t read_tpl = ib_clust_read_tuple_create(scan_crsr);
    assert(read_tpl != NULL);

    int64_t *final_keys = (int64_t *)calloc(ops, sizeof(int64_t));
    size_t final_count = 0;

    ib_err_t scan_err = ib_cursor_first(scan_crsr);
    if (scan_err == DB_SUCCESS) {
        while (1) {
            scan_err = ib_cursor_read_row(scan_crsr, read_tpl);
            if (scan_err == DB_END_OF_INDEX || scan_err == DB_RECORD_NOT_FOUND) {
                break;
            }
            if (scan_err != DB_SUCCESS) {
                fprintf(stderr, "Read failed: %s\n", ib_strerror(scan_err));
                return 1;
            }

            int value = 0;
            OK(ib_tuple_read_i32(read_tpl, 0, &value));
            final_keys[final_count++] = value;

            scan_err = ib_cursor_next(scan_crsr);
            if (scan_err == DB_END_OF_INDEX || scan_err == DB_RECORD_NOT_FOUND) {
                break;
            }
            if (scan_err != DB_SUCCESS) {
                fprintf(stderr, "Cursor next failed: %s\n", ib_strerror(scan_err));
                return 1;
            }

            read_tpl = ib_tuple_clear(read_tpl);
            assert(read_tpl != NULL);
        }
    } else if (scan_err != DB_END_OF_INDEX) {
        fprintf(stderr, "Cursor first failed: %s\n", ib_strerror(scan_err));
        return 1;
    }

    printf("final %zu", final_count);
    for (size_t i = 0; i < final_count; ++i) {
        printf(" %" PRId64, final_keys[i]);
    }
    printf("\n");

    if (read_tpl != NULL) {
        ib_tuple_delete(read_tpl);
    }
    free(final_keys);
    free(keys);

    OK(ib_cursor_close(scan_crsr));
    OK(ib_trx_commit(scan_trx));

    OK(drop_table(DATABASE, TABLE));
    OK(ib_shutdown(IB_SHUTDOWN_NORMAL));

    return 0;
}
