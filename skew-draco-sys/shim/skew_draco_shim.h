// skew-draco-sys: extern "C" shim over the vendored Google Draco 1.5.7
// point-cloud decoder (experiment 1-D registration §3-2).
//
// Contract: decode ONE Draco bitstream that encodes a point cloud with
// exactly two attributes, POSITION float32 x 3 and COLOR uint8 x 3 (what the
// experiment's DracoPy 2.0.0 encoder writes), and hand back the decoded
// positions and colours in Draco's point order. Nothing else (no rounding,
// no packing) happens here: the record conversion lives in Rust.
//
// No C++ exception crosses this boundary: every entry point catches
// everything and returns an error code. Single-threaded (Draco itself spawns
// no threads); a handle must not be used from two threads at once.

#ifndef SKEW_DRACO_SHIM_H_
#define SKEW_DRACO_SHIM_H_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SkewDracoCloud SkewDracoCloud;

// Error codes (stable; mirrored in Rust).
#define SKEW_DRACO_OK 0
#define SKEW_DRACO_ERR_NULL_ARGUMENT 1
#define SKEW_DRACO_ERR_NOT_A_POINT_CLOUD 2
#define SKEW_DRACO_ERR_DECODE 3
#define SKEW_DRACO_ERR_ATTRIBUTE_SET 4
#define SKEW_DRACO_ERR_POSITION_FORMAT 5
#define SKEW_DRACO_ERR_COLOR_FORMAT 6
#define SKEW_DRACO_ERR_BUFFER_SIZE 7
#define SKEW_DRACO_ERR_EXCEPTION 8
#define SKEW_DRACO_ERR_UNKNOWN_EXCEPTION 9

// Decode `len` bytes at `data`. On success returns SKEW_DRACO_OK and stores
// a new handle in *out (free it with skew_draco_cloud_free). On failure
// returns an error code, stores NULL in *out and, if `err` is not NULL and
// `err_len` > 0, a NUL-terminated message truncated to `err_len` bytes.
int skew_draco_decode_point_cloud(const uint8_t *data, size_t len,
                                  SkewDracoCloud **out, char *err,
                                  size_t err_len);

// Number of points of a decoded cloud (0 for NULL).
size_t skew_draco_cloud_num_points(const SkewDracoCloud *cloud);

// Copy the decoded values in point order: 3 floats per point into
// `positions` (exactly 3 * num_points floats) and 3 bytes per point into
// `colors` (exactly 3 * num_points bytes). The float bits are copied
// unchanged from the decoder's attribute buffer.
int skew_draco_cloud_copy(const SkewDracoCloud *cloud, float *positions,
                          size_t positions_len, uint8_t *colors,
                          size_t colors_len);

void skew_draco_cloud_free(SkewDracoCloud *cloud);

// The vendored library's own version string (draco::kDracoVersion).
const char *skew_draco_version(void);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // SKEW_DRACO_SHIM_H_
