use std::collections::HashMap;
use std::path::{Path, PathBuf};
use wgpu::*;
use super::lua_icons::{extract_lua_string, resolve_factorio_path};

pub struct TextureAtlas {
    pub texture: Texture,
    pub view: TextureView,
    pub sampler: Sampler,
    pub bind_group_layout: BindGroupLayout,
    pub bind_group: BindGroup,
    uvs: HashMap<String, [f32; 4]>, // entity_name -> [u0, v0, u1, v1]
}

const ATLAS_SIZE: u32 = 2048;
const ICON_SIZE: u32 = 64;
const SLOTS_PER_ROW: u32 = ATLAS_SIZE / ICON_SIZE; // 32

impl TextureAtlas {
    pub fn new(device: &Device, queue: &Queue, factorio_path: &Path) -> Self {
        let icon_paths = scan_icon_paths(factorio_path);

        let mut atlas_data = vec![0u8; (ATLAS_SIZE * ATLAS_SIZE * 4) as usize];
        let mut uvs = HashMap::new();
        let mut slot = 0u32;

        for (name, path) in &icon_paths {
            if slot >= SLOTS_PER_ROW * SLOTS_PER_ROW { break; }
            let img = match image::open(path) {
                Ok(img) => img,
                Err(_) => continue,
            };
            let icon = if img.width() > ICON_SIZE && img.height() >= ICON_SIZE {
                img.crop_imm(0, 0, ICON_SIZE, ICON_SIZE)
            } else {
                img
            };
            let rgba = icon.to_rgba8();

            let col = slot % SLOTS_PER_ROW;
            let row = slot / SLOTS_PER_ROW;
            let ox = col * ICON_SIZE;
            let oy = row * ICON_SIZE;

            for py in 0..rgba.height().min(ICON_SIZE) {
                for px in 0..rgba.width().min(ICON_SIZE) {
                    let src = &rgba.as_raw()[((py * rgba.width() + px) * 4) as usize..][..4];
                    let dst_idx = (((oy + py) * ATLAS_SIZE + ox + px) * 4) as usize;
                    atlas_data[dst_idx..dst_idx + 4].copy_from_slice(src);
                }
            }

            let u0 = ox as f32 / ATLAS_SIZE as f32;
            let v0 = oy as f32 / ATLAS_SIZE as f32;
            let u1 = (ox + ICON_SIZE) as f32 / ATLAS_SIZE as f32;
            let v1 = (oy + ICON_SIZE) as f32 / ATLAS_SIZE as f32;
            uvs.insert(name.clone(), [u0, v0, u1, v1]);
            slot += 1;
        }

        let texture = device.create_texture(&TextureDescriptor {
            label: Some("atlas"),
            size: Extent3d { width: ATLAS_SIZE, height: ATLAS_SIZE, depth_or_array_layers: 1 },
            mip_level_count: 1,
            sample_count: 1,
            dimension: TextureDimension::D2,
            format: TextureFormat::Rgba8Unorm,
            usage: TextureUsages::TEXTURE_BINDING | TextureUsages::COPY_DST,
            view_formats: &[],
        });

        queue.write_texture(
            TexelCopyTextureInfo { texture: &texture, mip_level: 0, origin: Origin3d::ZERO, aspect: TextureAspect::All },
            &atlas_data,
            TexelCopyBufferLayout { offset: 0, bytes_per_row: Some(ATLAS_SIZE * 4), rows_per_image: Some(ATLAS_SIZE) },
            Extent3d { width: ATLAS_SIZE, height: ATLAS_SIZE, depth_or_array_layers: 1 },
        );

        let view = texture.create_view(&TextureViewDescriptor::default());
        let sampler = device.create_sampler(&SamplerDescriptor {
            mag_filter: FilterMode::Linear,
            min_filter: FilterMode::Linear,
            ..Default::default()
        });

        let bind_group_layout = device.create_bind_group_layout(&BindGroupLayoutDescriptor {
            label: Some("atlas_bgl"),
            entries: &[
                BindGroupLayoutEntry {
                    binding: 0,
                    visibility: ShaderStages::FRAGMENT,
                    ty: BindingType::Texture {
                        sample_type: TextureSampleType::Float { filterable: true },
                        view_dimension: TextureViewDimension::D2,
                        multisampled: false,
                    },
                    count: None,
                },
                BindGroupLayoutEntry {
                    binding: 1,
                    visibility: ShaderStages::FRAGMENT,
                    ty: BindingType::Sampler(SamplerBindingType::Filtering),
                    count: None,
                },
            ],
        });

        let bind_group = device.create_bind_group(&BindGroupDescriptor {
            label: Some("atlas_bg"),
            layout: &bind_group_layout,
            entries: &[
                BindGroupEntry { binding: 0, resource: BindingResource::TextureView(&view) },
                BindGroupEntry { binding: 1, resource: BindingResource::Sampler(&sampler) },
            ],
        });

        Self { texture, view, sampler, bind_group_layout, bind_group, uvs }
    }

    pub fn get_uv(&self, entity_name: &str) -> Option<[f32; 4]> {
        self.uvs.get(entity_name).copied()
    }

    pub fn get_uv_or_fallback(&self, entity_name: &str) -> Option<[f32; 4]> {
        if let Some(uv) = self.get_uv(entity_name) {
            return Some(uv);
        }

        let fallback = if entity_name.contains("iron-ore") {
            "iron-ore"
        } else if entity_name.contains("copper-ore") {
            "copper-ore"
        } else if entity_name.contains("uranium") {
            "uranium-ore"
        } else if entity_name == "coal" {
            "coal"
        } else if entity_name == "stone" {
            "stone"
        } else if entity_name.contains("tree") || entity_name.contains("dead-") {
            "tree-01"
        } else if entity_name.contains("rock") {
            "big-rock"
        } else if entity_name.contains("fish") {
            "fish"
        } else {
            "unknown"
        };

        self.get_uv(fallback)
    }
}

fn scan_icon_paths(factorio_path: &Path) -> Vec<(String, PathBuf)> {
    let mut results = Vec::new();
    let proto_dir = factorio_path.join("base/prototypes");
    let lua_files = [
        "entity/entities.lua", "entity/transport-belts.lua", "entity/enemies.lua",
        "entity/trees.lua", "entity/turrets.lua", "entity/trains.lua",
        "entity/resources.lua", "entity/mining-drill.lua", "entity/flying-robots.lua",
        "decorative/decoratives.lua",
    ];

    for filename in &lua_files {
        let path = proto_dir.join(filename);
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => continue,
        };
        parse_lua_icons(&content, factorio_path, &mut results);
    }

    // Scan icon directories for direct PNGs
    let icon_dirs = [
        factorio_path.join("base/graphics/icons"),
        factorio_path.join("core/graphics/icons"),
        factorio_path.join("core/graphics/icons/entity"),
    ];
    for icons_dir in &icon_dirs {
        if let Ok(entries) = std::fs::read_dir(icons_dir) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.extension().map_or(false, |e| e == "png") {
                    if let Some(stem) = p.file_stem().and_then(|s| s.to_str()) {
                        let name = stem.to_string();
                        if !results.iter().any(|(n, _)| n == &name) {
                            results.push((name, p));
                        }
                    }
                }
            }
        }
    }

    results
}

fn parse_lua_icons(content: &str, factorio_path: &Path, results: &mut Vec<(String, PathBuf)>) {
    let mut current_name: Option<String> = None;

    for line in content.lines() {
        let trimmed = line.trim();

        if let Some(name) = extract_lua_string(trimmed, "name") {
            current_name = Some(name);
        }

        if let Some(icon_path) = extract_lua_string(trimmed, "icon") {
            if let Some(ref name) = current_name {
                if let Some(resolved) = resolve_factorio_path(&icon_path, factorio_path) {
                    if resolved.exists() && !results.iter().any(|(n, _)| n == name) {
                        results.push((name.clone(), resolved));
                    }
                }
            }
        }

        if trimmed == "}," || trimmed == "}" {
            current_name = None;
        }
    }
}
