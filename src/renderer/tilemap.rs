use wgpu::*;
use bytemuck::{Pod, Zeroable};

#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
pub struct TileInstance {
    pub pos: [f32; 2],
    pub color: [f32; 4],
}

const TILE_SHADER: &str = r#"
struct Camera {
    view_proj: mat4x4<f32>,
};
@group(0) @binding(0) var<uniform> camera: Camera;

struct Instance {
    @location(0) pos: vec2<f32>,
    @location(1) color: vec4<f32>,
};

struct VsOut {
    @builtin(position) pos: vec4<f32>,
    @location(0) color: vec4<f32>,
};

@vertex
fn vs_main(@builtin(vertex_index) vi: u32, inst: Instance) -> VsOut {
    let x = f32(vi & 1u);
    let y = f32((vi >> 1u) & 1u);
    let world = vec4<f32>(inst.pos.x + x, inst.pos.y + y, 0.0, 1.0);
    var out: VsOut;
    out.pos = camera.view_proj * world;
    out.color = inst.color;
    return out;
}

@fragment
fn fs_main(in: VsOut) -> @location(0) vec4<f32> {
    return in.color;
}
"#;

pub struct TilemapRenderer {
    pipeline: RenderPipeline,
    instance_buffer: Buffer,
    instance_count: u32,
    capacity: u64,
}

impl TilemapRenderer {
    pub fn new(device: &Device, format: TextureFormat, camera_bgl: &BindGroupLayout) -> Self {
        let shader = device.create_shader_module(ShaderModuleDescriptor {
            label: Some("tile_shader"),
            source: ShaderSource::Wgsl(TILE_SHADER.into()),
        });

        let layout = device.create_pipeline_layout(&PipelineLayoutDescriptor {
            label: Some("tile_pipeline_layout"),
            bind_group_layouts: &[camera_bgl],
            push_constant_ranges: &[],
        });

        let pipeline = device.create_render_pipeline(&RenderPipelineDescriptor {
            label: Some("tile_pipeline"),
            layout: Some(&layout),
            vertex: VertexState {
                module: &shader,
                entry_point: Some("vs_main"),
                buffers: &[VertexBufferLayout {
                    array_stride: std::mem::size_of::<TileInstance>() as u64,
                    step_mode: VertexStepMode::Instance,
                    attributes: &[
                        VertexAttribute { format: VertexFormat::Float32x2, offset: 0, shader_location: 0 },
                        VertexAttribute { format: VertexFormat::Float32x4, offset: 8, shader_location: 1 },
                    ],
                }],
                compilation_options: Default::default(),
            },
            fragment: Some(FragmentState {
                module: &shader,
                entry_point: Some("fs_main"),
                targets: &[Some(ColorTargetState {
                    format,
                    blend: None,
                    write_mask: ColorWrites::ALL,
                })],
                compilation_options: Default::default(),
            }),
            primitive: PrimitiveState {
                topology: PrimitiveTopology::TriangleStrip,
                strip_index_format: None,
                ..Default::default()
            },
            depth_stencil: None,
            multisample: MultisampleState::default(),
            multiview: None,
            cache: None,
        });

        let capacity = 256 * 1024;
        let instance_buffer = device.create_buffer(&BufferDescriptor {
            label: Some("tile_instances"),
            size: capacity * std::mem::size_of::<TileInstance>() as u64,
            usage: BufferUsages::VERTEX | BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        Self { pipeline, instance_buffer, instance_count: 0, capacity }
    }

    pub fn upload(&mut self, device: &Device, queue: &Queue, instances: &[TileInstance]) {
        let needed = instances.len() as u64;
        if needed > self.capacity {
            self.capacity = needed.next_power_of_two();
            self.instance_buffer = device.create_buffer(&BufferDescriptor {
                label: Some("tile_instances"),
                size: self.capacity * std::mem::size_of::<TileInstance>() as u64,
                usage: BufferUsages::VERTEX | BufferUsages::COPY_DST,
                mapped_at_creation: false,
            });
        }
        queue.write_buffer(&self.instance_buffer, 0, bytemuck::cast_slice(instances));
        self.instance_count = instances.len() as u32;
    }

    pub fn draw<'a>(&'a self, pass: &mut RenderPass<'a>, camera_bg: &'a BindGroup) {
        if self.instance_count == 0 { return; }
        pass.set_pipeline(&self.pipeline);
        pass.set_bind_group(0, camera_bg, &[]);
        pass.set_vertex_buffer(0, self.instance_buffer.slice(..));
        pass.draw(0..4, 0..self.instance_count);
    }
}

pub fn tile_color(name: &str) -> [f32; 4] {
    let (r, g, b) = match name {
        n if n.contains("deepwater") => (20, 50, 100),
        n if n.contains("water") => (40, 80, 140),
        n if n.contains("grass-1") => (60, 100, 40),
        n if n.contains("grass-2") => (70, 110, 45),
        n if n.contains("grass-3") => (80, 120, 50),
        n if n.contains("grass-4") => (90, 130, 55),
        n if n.contains("grass") => (70, 110, 45),
        n if n.contains("dry-dirt") => (140, 110, 70),
        n if n.contains("dirt-1") => (100, 70, 40),
        n if n.contains("dirt-2") => (110, 75, 45),
        n if n.contains("dirt-3") => (115, 80, 50),
        n if n.contains("dirt-4") => (120, 85, 55),
        n if n.contains("dirt-5") => (125, 90, 55),
        n if n.contains("dirt-6") => (130, 95, 60),
        n if n.contains("dirt-7") => (135, 100, 65),
        n if n.contains("dirt") => (110, 80, 50),
        n if n.contains("red-desert") => (150, 90, 60),
        n if n.contains("sand-1") => (180, 160, 100),
        n if n.contains("sand-2") => (190, 170, 110),
        n if n.contains("sand-3") => (200, 180, 120),
        n if n.contains("sand") => (190, 170, 110),
        n if n.contains("stone-path") => (100, 100, 100),
        n if n.contains("concrete") => (120, 120, 120),
        n if n.contains("refined-concrete") => (140, 140, 140),
        n if n.contains("landfill") => (90, 85, 70),
        n if n.contains("out-of-map") || n.contains("empty-space") => (10, 10, 15),
        _ => (60, 60, 60),
    };
    [r as f32 / 255.0, g as f32 / 255.0, b as f32 / 255.0, 1.0]
}
