use wgpu::*;

pub struct GpuState {
    pub device: Device,
    pub queue: Queue,
    pub format: TextureFormat,
    pub width: u32,
    pub height: u32,
    pub camera_buffer: Buffer,
    pub camera_bind_group_layout: BindGroupLayout,
    pub camera_bind_group: BindGroup,
    render_texture: Texture,
    render_view: TextureView,
    readback_buffer: Buffer,
    row_stride: u32,
}

impl GpuState {
    pub fn new(width: u32, height: u32) -> Self {
        let instance = Instance::new(&InstanceDescriptor {
            backends: Backends::PRIMARY,
            ..Default::default()
        });
        let adapter = pollster::block_on(instance.request_adapter(&RequestAdapterOptions {
            power_preference: PowerPreference::HighPerformance,
            compatible_surface: None,
            force_fallback_adapter: false,
        }))
        .unwrap();

        let (device, queue) = pollster::block_on(adapter.request_device(&DeviceDescriptor {
            label: Some("factorio-gpu"),
            required_features: Features::empty(),
            required_limits: Limits::default(),
            ..Default::default()
        }, None))
        .unwrap();

        let format = TextureFormat::Rgba8Unorm;

        let camera_buffer = device.create_buffer(&BufferDescriptor {
            label: Some("camera_uniform"),
            size: 64,
            usage: BufferUsages::UNIFORM | BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        let camera_bind_group_layout = device.create_bind_group_layout(&BindGroupLayoutDescriptor {
            label: Some("camera_bgl"),
            entries: &[BindGroupLayoutEntry {
                binding: 0,
                visibility: ShaderStages::VERTEX,
                ty: BindingType::Buffer {
                    ty: BufferBindingType::Uniform,
                    has_dynamic_offset: false,
                    min_binding_size: None,
                },
                count: None,
            }],
        });

        let camera_bind_group = device.create_bind_group(&BindGroupDescriptor {
            label: Some("camera_bg"),
            layout: &camera_bind_group_layout,
            entries: &[BindGroupEntry {
                binding: 0,
                resource: camera_buffer.as_entire_binding(),
            }],
        });

        let (render_texture, render_view, readback_buffer, row_stride) =
            create_render_target(&device, width, height, format);

        Self {
            device, queue, format, width, height,
            camera_buffer, camera_bind_group_layout, camera_bind_group,
            render_texture, render_view, readback_buffer, row_stride,
        }
    }

    pub fn resize(&mut self, width: u32, height: u32) {
        if width == self.width && height == self.height { return; }
        self.width = width;
        self.height = height;
        let (tex, view, buf, stride) =
            create_render_target(&self.device, width, height, self.format);
        self.render_texture = tex;
        self.render_view = view;
        self.readback_buffer = buf;
        self.row_stride = stride;
    }

    pub fn upload_camera(&self, view_proj: &[[f32; 4]; 4]) {
        self.queue.write_buffer(&self.camera_buffer, 0, bytemuck::cast_slice(view_proj.as_flattened()));
    }

    pub fn render_view(&self) -> &TextureView {
        &self.render_view
    }

    pub fn readback(&self, pixels: &mut Vec<u8>) {
        let mut encoder = self.device.create_command_encoder(&CommandEncoderDescriptor { label: None });
        encoder.copy_texture_to_buffer(
            TexelCopyTextureInfo {
                texture: &self.render_texture,
                mip_level: 0,
                origin: Origin3d::ZERO,
                aspect: TextureAspect::All,
            },
            TexelCopyBufferInfo {
                buffer: &self.readback_buffer,
                layout: TexelCopyBufferLayout {
                    offset: 0,
                    bytes_per_row: Some(self.row_stride),
                    rows_per_image: Some(self.height),
                },
            },
            Extent3d { width: self.width, height: self.height, depth_or_array_layers: 1 },
        );
        self.queue.submit(std::iter::once(encoder.finish()));

        let slice = self.readback_buffer.slice(..);
        slice.map_async(MapMode::Read, |_| {});
        self.device.poll(Maintain::Wait);

        let data = slice.get_mapped_range();
        pixels.clear();
        pixels.reserve((self.width * self.height * 4) as usize);
        for row in 0..self.height {
            let start = (row * self.row_stride) as usize;
            let end = start + (self.width * 4) as usize;
            pixels.extend_from_slice(&data[start..end]);
        }
        drop(data);
        self.readback_buffer.unmap();
    }
}

fn create_render_target(device: &Device, width: u32, height: u32, format: TextureFormat) -> (Texture, TextureView, Buffer, u32) {
    let texture = device.create_texture(&TextureDescriptor {
        label: Some("render_target"),
        size: Extent3d { width, height, depth_or_array_layers: 1 },
        mip_level_count: 1,
        sample_count: 1,
        dimension: TextureDimension::D2,
        format,
        usage: TextureUsages::RENDER_ATTACHMENT | TextureUsages::COPY_SRC,
        view_formats: &[],
    });
    let view = texture.create_view(&TextureViewDescriptor::default());

    // Row stride must be aligned to 256 bytes for buffer copies
    let unpadded = width * 4;
    let row_stride = (unpadded + 255) & !255;

    let readback_buffer = device.create_buffer(&BufferDescriptor {
        label: Some("readback"),
        size: (row_stride * height) as u64,
        usage: BufferUsages::COPY_DST | BufferUsages::MAP_READ,
        mapped_at_creation: false,
    });

    (texture, view, readback_buffer, row_stride)
}
