//! Builds the vendored Google Draco 1.5.7 point-cloud decoder plus the
//! extern "C" shim with FIXED flags (registration 1-D §3-2):
//! `-std=c++17 -O3 -DNDEBUG`, plus `-fPIC` (the objects are linked into PIE
//! executables) and `-ffp-contract=off` (no fused multiply-add: the
//! dequantization `value * delta + min` must round exactly like the encoder
//! side's library; baseline x86-64 has no FMA instructions anyway, so this
//! only guards against a target that has them). Never `-march=native`, never
//! `-ffast-math`, in every cargo profile (cc's profile-derived defaults are
//! disabled, so a debug build is still `-O3`).
//!
//! The compiler may be chosen with `CXX` (cc's rule) but its identity is
//! recorded; extra flags from the environment (`CXXFLAGS` and its target /
//! host variants) are REFUSED so that the freeze can rely on the flag list.
//! The compiler path, its `--version` first line, the exact argument vector
//! and a digest of every vendored/shim source become compile-time constants
//! of the crate (`DecoderInfo`) for the L2 meta row.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// Draco's own decoder-only source list (CMakeLists.txt, the Emscripten
/// `draco_decoder_src` groups without the JS glue), minus the two
/// transcoder-only mesh files (`mesh_splitter.cc`, `mesh_utils.cc`, appended
/// only under DRACO_TRANSCODER_SUPPORTED, which is off).
const DRACO_SOURCES: &[&str] = &[
    "src/draco/attributes/attribute_octahedron_transform.cc",
    "src/draco/attributes/attribute_quantization_transform.cc",
    "src/draco/attributes/attribute_transform.cc",
    "src/draco/attributes/geometry_attribute.cc",
    "src/draco/attributes/point_attribute.cc",
    "src/draco/compression/attributes/attributes_decoder.cc",
    "src/draco/compression/attributes/kd_tree_attributes_decoder.cc",
    "src/draco/compression/attributes/sequential_attribute_decoder.cc",
    "src/draco/compression/attributes/sequential_attribute_decoders_controller.cc",
    "src/draco/compression/attributes/sequential_integer_attribute_decoder.cc",
    "src/draco/compression/attributes/sequential_normal_attribute_decoder.cc",
    "src/draco/compression/attributes/sequential_quantization_attribute_decoder.cc",
    "src/draco/compression/bit_coders/adaptive_rans_bit_decoder.cc",
    "src/draco/compression/bit_coders/adaptive_rans_bit_encoder.cc",
    "src/draco/compression/bit_coders/direct_bit_decoder.cc",
    "src/draco/compression/bit_coders/direct_bit_encoder.cc",
    "src/draco/compression/bit_coders/rans_bit_decoder.cc",
    "src/draco/compression/bit_coders/rans_bit_encoder.cc",
    "src/draco/compression/bit_coders/symbol_bit_decoder.cc",
    "src/draco/compression/bit_coders/symbol_bit_encoder.cc",
    "src/draco/compression/decode.cc",
    "src/draco/compression/draco_compression_options.cc",
    "src/draco/compression/entropy/shannon_entropy.cc",
    "src/draco/compression/entropy/symbol_decoding.cc",
    "src/draco/compression/entropy/symbol_encoding.cc",
    "src/draco/compression/mesh/mesh_decoder.cc",
    "src/draco/compression/mesh/mesh_edgebreaker_decoder.cc",
    "src/draco/compression/mesh/mesh_edgebreaker_decoder_impl.cc",
    "src/draco/compression/mesh/mesh_sequential_decoder.cc",
    "src/draco/compression/point_cloud/algorithms/dynamic_integer_points_kd_tree_decoder.cc",
    "src/draco/compression/point_cloud/algorithms/float_points_tree_decoder.cc",
    "src/draco/compression/point_cloud/point_cloud_decoder.cc",
    "src/draco/compression/point_cloud/point_cloud_kd_tree_decoder.cc",
    "src/draco/compression/point_cloud/point_cloud_sequential_decoder.cc",
    "src/draco/core/bit_utils.cc",
    "src/draco/core/bounding_box.cc",
    "src/draco/core/cycle_timer.cc",
    "src/draco/core/data_buffer.cc",
    "src/draco/core/decoder_buffer.cc",
    "src/draco/core/divide.cc",
    "src/draco/core/draco_types.cc",
    "src/draco/core/encoder_buffer.cc",
    "src/draco/core/hash_utils.cc",
    "src/draco/core/options.cc",
    "src/draco/core/quantization_utils.cc",
    "src/draco/mesh/corner_table.cc",
    "src/draco/mesh/mesh.cc",
    "src/draco/mesh/mesh_are_equivalent.cc",
    "src/draco/mesh/mesh_attribute_corner_table.cc",
    "src/draco/mesh/mesh_cleanup.cc",
    "src/draco/mesh/mesh_features.cc",
    "src/draco/mesh/mesh_misc_functions.cc",
    "src/draco/mesh/mesh_stripifier.cc",
    "src/draco/mesh/triangle_soup_mesh_builder.cc",
    "src/draco/metadata/geometry_metadata.cc",
    "src/draco/metadata/metadata.cc",
    "src/draco/metadata/metadata_decoder.cc",
    "src/draco/metadata/property_attribute.cc",
    "src/draco/metadata/property_table.cc",
    "src/draco/metadata/structural_metadata.cc",
    "src/draco/metadata/structural_metadata_schema.cc",
    "src/draco/point_cloud/point_cloud.cc",
    "src/draco/point_cloud/point_cloud_builder.cc",
];

/// The registered flags, in the order they are passed (the include paths and
/// the source file follow them on every compiler command line).
const FIXED_FLAGS: &[&str] = &["-std=c++17", "-O3", "-DNDEBUG", "-fPIC", "-ffp-contract=off"];

/// Environment variables through which `cc` would append compiler flags or
/// swap the C++ runtime it links (`CXXSTDLIB`).
fn refused_flag_variables(target: &str) -> Vec<String> {
    let mut vars = Vec::new();
    for base in ["CXXFLAGS", "CXXSTDLIB"] {
        vars.push(base.to_string());
        vars.push(format!("TARGET_{base}"));
        vars.push(format!("HOST_{base}"));
        vars.push(format!("{base}_{target}"));
        vars.push(format!("{base}_{}", target.replace('-', "_")));
    }
    vars
}

fn collect_files(dir: &Path, root: &Path, out: &mut Vec<String>) {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .map(|e| e.expect("dir entry").path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            collect_files(&path, root, out);
        } else {
            let rel = path.strip_prefix(root).expect("under the crate root");
            out.push(rel.to_str().expect("UTF-8 path").to_string());
        }
    }
}

/// `sha256sum`-format digest of every file under `vendor/` and `shim/`
/// (paths relative to the crate root, byte-sorted): reproducible with
/// `(cd skew-draco-sys && find vendor shim -type f | LC_ALL=C sort | xargs sha256sum) | sha256sum`.
fn source_digest(root: &Path) -> (String, usize) {
    let mut files = Vec::new();
    collect_files(&root.join("vendor"), root, &mut files);
    collect_files(&root.join("shim"), root, &mut files);
    files.sort();
    let mut listing = String::new();
    for rel in &files {
        let bytes = std::fs::read(root.join(rel)).unwrap_or_else(|e| panic!("read {rel}: {e}"));
        let digest = Sha256::digest(&bytes);
        listing.push_str(&format!("{}  {rel}\n", hex(&digest)));
    }
    (hex(&Sha256::digest(listing.as_bytes())), files.len())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn main() {
    let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("cargo sets CARGO_MANIFEST_DIR"));
    let target = std::env::var("TARGET").expect("cargo sets TARGET");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=vendor");
    println!("cargo:rerun-if-changed=shim");
    println!("cargo:rerun-if-env-changed=CXX");
    for var in refused_flag_variables(&target) {
        println!("cargo:rerun-if-env-changed={var}");
        if std::env::var_os(&var).is_some_and(|v| !v.is_empty()) {
            panic!(
                "skew-draco-sys: {var} is set; the Draco decoder is built with fixed flags only \
                 ({}) — unset it",
                FIXED_FLAGS.join(" ")
            );
        }
    }

    let draco = root.join("vendor/draco-1.5.7");
    let mut build = cc::Build::new();
    build
        .cpp(true)
        // Codex 112 P2-2: no flag inherited from RUSTFLAGS either.
        .inherit_rustflags(false)
        .no_default_flags(true)
        .warnings(false)
        .extra_warnings(false)
        .include(root.join("vendor/gen"))
        .include(draco.join("src"))
        .include(root.join("shim"));
    for flag in FIXED_FLAGS {
        build.flag(flag);
    }
    for source in DRACO_SOURCES {
        build.file(draco.join(source));
    }
    build.file(root.join("shim/skew_draco_shim.cc"));

    // Record exactly what compiles the sources, and refuse ANY argument
    // outside the allowlist: the three include paths and the fixed flags,
    // in this order (the per-file `-o <obj> -c <src>` follow them).
    let tool = build.get_compiler();
    let args: Vec<String> = tool.args().iter().map(|a| a.to_string_lossy().into_owned()).collect();
    let mut allowed: Vec<String> = Vec::new();
    for dir in [root.join("vendor/gen"), draco.join("src"), root.join("shim")] {
        allowed.push("-I".into());
        allowed.push(dir.to_str().expect("UTF-8 include path").to_string());
    }
    allowed.extend(FIXED_FLAGS.iter().map(|f| f.to_string()));
    if args != allowed {
        let extra: Vec<&String> = args.iter().filter(|a| !allowed.contains(a)).collect();
        let missing: Vec<&String> = allowed.iter().filter(|a| !args.contains(a)).collect();
        panic!(
            "skew-draco-sys: the compiler arguments are not exactly the registered ones\n  got:     {args:?}\n  allowed: {allowed:?}\n  extra:   {extra:?}\n  missing: {missing:?}"
        );
    }
    let version = std::process::Command::new(tool.path())
        .arg("--version")
        .output()
        .ok()
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .and_then(|text| text.lines().next().map(str::to_string))
        .unwrap_or_else(|| "unknown".into());
    // cc links libstdc++ (dynamic). Every binary of a dependent package
    // then has libstdc++.so.6 as NEEDED, including ones that never decode:
    // their link pulls Draco objects through shared codegen units, and the
    // exception tables that survive --gc-sections reference the C++
    // personality (a link-modifier `as-needed` is nightly-only).
    build.compile("skew_draco");

    let (digest, files) = source_digest(&root);
    // The include paths are absolute (machine-specific); record them
    // relative to the crate so the recorded vector is reproducible.
    let root_str = root.to_str().expect("UTF-8 crate path");
    let args_rel: Vec<String> = args.iter().map(|a| a.replace(root_str, "<crate>")).collect();
    println!("cargo:rustc-env=SKEW_DRACO_CXX={}", tool.path().display());
    println!("cargo:rustc-env=SKEW_DRACO_CXX_VERSION={version}");
    println!("cargo:rustc-env=SKEW_DRACO_CXX_ARGS={}", args_rel.join(" "));
    println!("cargo:rustc-env=SKEW_DRACO_FIXED_FLAGS={}", FIXED_FLAGS.join(" "));
    println!("cargo:rustc-env=SKEW_DRACO_SOURCE_SHA256={digest}");
    println!("cargo:rustc-env=SKEW_DRACO_SOURCE_FILES={files}");
    println!("cargo:rustc-env=SKEW_DRACO_COMPILED_SOURCES={}", DRACO_SOURCES.len() + 1);
    // The Rust side (record packing) of this crate: its optimisation level in
    // the profile being built (a workspace profile override sets 3 for dev).
    println!(
        "cargo:rustc-env=SKEW_DRACO_RUST_OPT_LEVEL={}",
        std::env::var("OPT_LEVEL").unwrap_or_else(|_| "unknown".into())
    );
}
