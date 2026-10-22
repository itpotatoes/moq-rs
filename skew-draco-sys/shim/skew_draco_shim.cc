// skew-draco-sys extern "C" shim; see skew_draco_shim.h for the contract.

#include "skew_draco_shim.h"

#include <dlfcn.h>

#include <cstring>
#include <exception>
#include <memory>
#include <new>
#include <string>

#include "draco/attributes/geometry_attribute.h"
#include "draco/attributes/point_attribute.h"
#include "draco/compression/config/compression_shared.h"
#include "draco/compression/decode.h"
#include "draco/core/decoder_buffer.h"
#include "draco/core/draco_types.h"
#include "draco/core/draco_version.h"
#include "draco/point_cloud/point_cloud.h"

struct SkewDracoCloud {
  std::unique_ptr<draco::PointCloud> cloud;
  const draco::PointAttribute *position = nullptr;
  const draco::PointAttribute *color = nullptr;
};

namespace {

void set_error(char *err, size_t err_len, const char *message) {
  if (err == nullptr || err_len == 0) return;
  const size_t n = std::strlen(message);
  const size_t k = n < err_len - 1 ? n : err_len - 1;
  std::memcpy(err, message, k);
  err[k] = '\0';
}

int fail(char *err, size_t err_len, int code, const std::string &message) {
  set_error(err, err_len, message.c_str());
  return code;
}

int decode(const uint8_t *data, size_t len, SkewDracoCloud **out, char *err,
           size_t err_len) {
  draco::DecoderBuffer buffer;
  buffer.Init(reinterpret_cast<const char *>(data), len);
  // The geometry type is read from a copy of the buffer.
  auto type = draco::Decoder::GetEncodedGeometryType(&buffer);
  if (!type.ok()) {
    return fail(err, err_len, SKEW_DRACO_ERR_DECODE,
                std::string("header: ") + type.status().error_msg());
  }
  if (type.value() != draco::POINT_CLOUD) {
    return fail(err, err_len, SKEW_DRACO_ERR_NOT_A_POINT_CLOUD,
                "the bitstream does not encode a point cloud");
  }
  draco::Decoder decoder;
  auto decoded = decoder.DecodePointCloudFromBuffer(&buffer);
  if (!decoded.ok()) {
    return fail(err, err_len, SKEW_DRACO_ERR_DECODE,
                std::string("decode: ") + decoded.status().error_msg());
  }
  std::unique_ptr<draco::PointCloud> cloud = std::move(decoded).value();
  if (cloud == nullptr) {
    return fail(err, err_len, SKEW_DRACO_ERR_DECODE, "decode: no cloud");
  }
  // Exactly POSITION + COLOR, one of each.
  if (cloud->num_attributes() != 2 ||
      cloud->NumNamedAttributes(draco::GeometryAttribute::POSITION) != 1 ||
      cloud->NumNamedAttributes(draco::GeometryAttribute::COLOR) != 1) {
    return fail(err, err_len, SKEW_DRACO_ERR_ATTRIBUTE_SET,
                "expected exactly one POSITION and one COLOR attribute, got " +
                    std::to_string(cloud->num_attributes()) + " attributes");
  }
  const draco::PointAttribute *position =
      cloud->GetNamedAttribute(draco::GeometryAttribute::POSITION);
  const draco::PointAttribute *color =
      cloud->GetNamedAttribute(draco::GeometryAttribute::COLOR);
  if (position == nullptr || position->num_components() != 3 ||
      position->data_type() != draco::DT_FLOAT32 ||
      position->byte_stride() != 3 * static_cast<int64_t>(sizeof(float))) {
    return fail(err, err_len, SKEW_DRACO_ERR_POSITION_FORMAT,
                "POSITION is not float32 x 3");
  }
  if (color == nullptr || color->num_components() != 3 ||
      color->data_type() != draco::DT_UINT8 || color->byte_stride() != 3) {
    return fail(err, err_len, SKEW_DRACO_ERR_COLOR_FORMAT,
                "COLOR is not uint8 x 3");
  }
  SkewDracoCloud *handle = new SkewDracoCloud();
  handle->cloud = std::move(cloud);
  handle->position = position;
  handle->color = color;
  *out = handle;
  return SKEW_DRACO_OK;
}

}  // namespace

extern "C" int skew_draco_decode_point_cloud(const uint8_t *data, size_t len,
                                             SkewDracoCloud **out, char *err,
                                             size_t err_len) {
  if (out == nullptr) {
    set_error(err, err_len, "null output handle");
    return SKEW_DRACO_ERR_NULL_ARGUMENT;
  }
  *out = nullptr;
  if (data == nullptr && len > 0) {
    set_error(err, err_len, "null input with a non-zero length");
    return SKEW_DRACO_ERR_NULL_ARGUMENT;
  }
  try {
    return decode(data, len, out, err, err_len);
  } catch (const std::exception &e) {
    // A handle stored before the throw cannot exist: decode() stores *out
    // as its last step, after which nothing can throw.
    *out = nullptr;
    set_error(err, err_len, e.what());
    return SKEW_DRACO_ERR_EXCEPTION;
  } catch (...) {
    *out = nullptr;
    set_error(err, err_len, "unknown C++ exception");
    return SKEW_DRACO_ERR_UNKNOWN_EXCEPTION;
  }
}

extern "C" size_t skew_draco_cloud_num_points(const SkewDracoCloud *cloud) {
  if (cloud == nullptr || cloud->cloud == nullptr) return 0;
  return static_cast<size_t>(cloud->cloud->num_points());
}

extern "C" int skew_draco_cloud_copy(const SkewDracoCloud *cloud,
                                     float *positions, size_t positions_len,
                                     uint8_t *colors, size_t colors_len) {
  if (cloud == nullptr || cloud->cloud == nullptr || positions == nullptr ||
      colors == nullptr) {
    return SKEW_DRACO_ERR_NULL_ARGUMENT;
  }
  try {
    const size_t n = static_cast<size_t>(cloud->cloud->num_points());
    if (positions_len != 3 * n || colors_len != 3 * n) {
      return SKEW_DRACO_ERR_BUFFER_SIZE;
    }
    for (draco::PointIndex i(0); i < cloud->cloud->num_points(); ++i) {
      const size_t k = static_cast<size_t>(i.value()) * 3;
      cloud->position->GetMappedValue(i, positions + k);
      cloud->color->GetMappedValue(i, colors + k);
    }
    return SKEW_DRACO_OK;
  } catch (...) {
    return SKEW_DRACO_ERR_UNKNOWN_EXCEPTION;
  }
}

extern "C" void skew_draco_cloud_free(SkewDracoCloud *cloud) {
  delete cloud;  // ~unique_ptr never throws
}

extern "C" const char *skew_draco_version(void) { return draco::kDracoVersion; }

#define SKEW_STR2(x) #x
#define SKEW_STR(x) SKEW_STR2(x)

extern "C" const char *skew_draco_cxx_runtime_build(void) {
#if defined(_GLIBCXX_RELEASE) && defined(__GLIBCXX__)
  return "libstdc++ _GLIBCXX_RELEASE=" SKEW_STR(_GLIBCXX_RELEASE) " __GLIBCXX__=" SKEW_STR(__GLIBCXX__);
#elif defined(_LIBCPP_VERSION)
  return "libc++ _LIBCPP_VERSION=" SKEW_STR(_LIBCPP_VERSION);
#else
  return "unknown";
#endif
}

extern "C" const char *skew_draco_cxx_runtime_loaded(void) {
  Dl_info info;
  void (*terminate)() noexcept = &std::terminate;
  if (dladdr(reinterpret_cast<void *>(terminate), &info) != 0 && info.dli_fname != nullptr) {
    return info.dli_fname;
  }
  return "unknown";
}
