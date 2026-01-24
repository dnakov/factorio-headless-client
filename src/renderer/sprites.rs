use wgpu::*;
use bytemuck::{Pod, Zeroable};

#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
pub struct SpriteInstance {
    pub pos: [f32; 2],
    pub size: [f32; 2],
    pub uv_min: [f32; 2],
    pub uv_max: [f32; 2],
    pub rotation: f32,
    pub _pad: f32,
}

const SPRITE_SHADER: &str = r#"
struct Camera {
    view_proj: mat4x4<f32>,
};
@group(0) @binding(0) var<uniform> camera: Camera;
@group(1) @binding(0) var atlas_tex: texture_2d<f32>;
@group(1) @binding(1) var atlas_samp: sampler;

struct Instance {
    @location(0) pos: vec2<f32>,
    @location(1) size: vec2<f32>,
    @location(2) uv_min: vec2<f32>,
    @location(3) uv_max: vec2<f32>,
    @location(4) rotation: f32,
};

struct VsOut {
    @builtin(position) pos: vec4<f32>,
    @location(0) uv: vec2<f32>,
};

@vertex
fn vs_main(@builtin(vertex_index) vi: u32, inst: Instance) -> VsOut {
    let x = f32(vi & 1u) - 0.5;
    let y = f32((vi >> 1u) & 1u) - 0.5;
    let c = cos(inst.rotation);
    let s = sin(inst.rotation);
    let rx = x * c - y * s;
    let ry = x * s + y * c;
    let world = vec4<f32>(
        inst.pos.x + rx * inst.size.x,
        inst.pos.y + ry * inst.size.y,
        0.0, 1.0
    );
    var out: VsOut;
    out.pos = camera.view_proj * world;
    let u = f32(vi & 1u);
    let v = f32((vi >> 1u) & 1u);
    out.uv = mix(inst.uv_min, inst.uv_max, vec2<f32>(u, v));
    return out;
}

@fragment
fn fs_main(in: VsOut) -> @location(0) vec4<f32> {
    let color = textureSample(atlas_tex, atlas_samp, in.uv);
    if color.a < 0.1 {
        discard;
    }
    return color;
}
"#;

pub struct SpriteRenderer {
    pipeline: RenderPipeline,
    instance_buffer: Buffer,
    instance_count: u32,
    capacity: u64,
}

impl SpriteRenderer {
    pub fn new(device: &Device, format: TextureFormat, camera_bgl: &BindGroupLayout, atlas_bgl: &BindGroupLayout) -> Self {
        let shader = device.create_shader_module(ShaderModuleDescriptor {
            label: Some("sprite_shader"),
            source: ShaderSource::Wgsl(SPRITE_SHADER.into()),
        });

        let layout = device.create_pipeline_layout(&PipelineLayoutDescriptor {
            label: Some("sprite_pipeline_layout"),
            bind_group_layouts: &[camera_bgl, atlas_bgl],
            push_constant_ranges: &[],
        });

        let pipeline = device.create_render_pipeline(&RenderPipelineDescriptor {
            label: Some("sprite_pipeline"),
            layout: Some(&layout),
            vertex: VertexState {
                module: &shader,
                entry_point: Some("vs_main"),
                buffers: &[VertexBufferLayout {
                    array_stride: std::mem::size_of::<SpriteInstance>() as u64,
                    step_mode: VertexStepMode::Instance,
                    attributes: &[
                        VertexAttribute { format: VertexFormat::Float32x2, offset: 0, shader_location: 0 },
                        VertexAttribute { format: VertexFormat::Float32x2, offset: 8, shader_location: 1 },
                        VertexAttribute { format: VertexFormat::Float32x2, offset: 16, shader_location: 2 },
                        VertexAttribute { format: VertexFormat::Float32x2, offset: 24, shader_location: 3 },
                        VertexAttribute { format: VertexFormat::Float32, offset: 32, shader_location: 4 },
                    ],
                }],
                compilation_options: Default::default(),
            },
            fragment: Some(FragmentState {
                module: &shader,
                entry_point: Some("fs_main"),
                targets: &[Some(ColorTargetState {
                    format,
                    blend: Some(BlendState::ALPHA_BLENDING),
                    write_mask: ColorWrites::ALL,
                })],
                compilation_options: Default::default(),
            }),
            primitive: PrimitiveState {
                topology: PrimitiveTopology::TriangleStrip,
                ..Default::default()
            },
            depth_stencil: None,
            multisample: MultisampleState::default(),
            multiview: None,
            cache: None,
        });

        let capacity = 64 * 1024;
        let instance_buffer = device.create_buffer(&BufferDescriptor {
            label: Some("sprite_instances"),
            size: capacity * std::mem::size_of::<SpriteInstance>() as u64,
            usage: BufferUsages::VERTEX | BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        Self { pipeline, instance_buffer, instance_count: 0, capacity }
    }

    pub fn upload(&mut self, device: &Device, queue: &Queue, instances: &[SpriteInstance]) {
        let needed = instances.len() as u64;
        if needed > self.capacity {
            self.capacity = needed.next_power_of_two();
            self.instance_buffer = device.create_buffer(&BufferDescriptor {
                label: Some("sprite_instances"),
                size: self.capacity * std::mem::size_of::<SpriteInstance>() as u64,
                usage: BufferUsages::VERTEX | BufferUsages::COPY_DST,
                mapped_at_creation: false,
            });
        }
        queue.write_buffer(&self.instance_buffer, 0, bytemuck::cast_slice(instances));
        self.instance_count = instances.len() as u32;
    }

    pub fn draw<'a>(&'a self, pass: &mut RenderPass<'a>, camera_bg: &'a BindGroup, atlas_bg: &'a BindGroup) {
        if self.instance_count == 0 { return; }
        pass.set_pipeline(&self.pipeline);
        pass.set_bind_group(0, camera_bg, &[]);
        pass.set_bind_group(1, atlas_bg, &[]);
        pass.set_vertex_buffer(0, self.instance_buffer.slice(..));
        pass.draw(0..4, 0..self.instance_count);
    }
}
