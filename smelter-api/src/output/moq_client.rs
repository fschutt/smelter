use std::{collections::HashMap, sync::Arc};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::*;

#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct MoqClientOutput {
    /// URL of the MoQ relay to publish to. Must use the `https://` scheme.
    pub endpoint_url: Arc<str>,
    /// Path of the broadcast to subscribe to on the relay.
    pub broadcast_path: Arc<str>,
    /// (**default=`false`**) Skips validation of the relay's TLS certificate.
    /// Only enable this on trusted networks — it leaves the connection vulnerable
    /// to man-in-the-middle attacks.
    pub disable_tls_verification: Option<bool>,
    /// Name of under which video track is advertised in the catalog.
    pub video_track: Option<Arc<str>>,
    /// Name of under which audio track is advertised in the catalog.
    pub audio_track: Option<Arc<str>>,
    /// Video stream configuration.
    pub video: Option<OutputMoqClientVideoOptions>,
    /// Audio stream configuration.
    pub audio: Option<OutputMoqClientAudioOptions>,
}

#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct OutputMoqClientVideoOptions {
    /// Output resolution in pixels.
    pub resolution: Resolution,
    /// Condition for termination of the output stream based on the input streams states. If output includes both audio and video streams, then EOS needs to be sent for every type.
    pub send_eos_when: Option<OutputEndCondition>,
    /// Video encoder options.
    pub encoder: MoqClientVideoEncoderOptions,
    /// Root of a component tree/scene that should be rendered for the output. Use [`update_output` request](../routes.md#update-output) to update this value after registration. [Learn more](../../concept/component.md).
    pub initial: VideoScene,
}

#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum MoqClientVideoEncoderOptions {
    #[serde(rename = "ffmpeg_h264")]
    FfmpegH264 {
        /// (**default=`"fast"`**) Video output encoder preset. Visit `FFmpeg` [docs](https://trac.ffmpeg.org/wiki/Encode/H.264#Preset) to learn more.
        preset: Option<H264EncoderPreset>,

        /// Encoding bitrate. Default value depends on chosen encoder.
        bitrate: Option<VideoEncoderBitrate>,

        /// (**default=`5000`**) Maximal interval between keyframes, in milliseconds.
        keyframe_interval_ms: Option<f64>,

        /// (**default=`"yuv420p"`**) Encoder pixel format
        pixel_format: Option<PixelFormat>,

        /// Raw FFmpeg encoder options. See [docs](https://ffmpeg.org/ffmpeg-codecs.html) for more.
        ffmpeg_options: Option<HashMap<Arc<str>, Arc<str>>>,
    },
    #[serde(rename = "ffmpeg_vp8")]
    FfmpegVp8 {
        /// Encoding bitrate. If not provided, bitrate is calculated based on resolution and framerate.
        /// For example at 1080p 30 FPS the average bitrate is 5000 kbit/s and max bitrate is 6250 kbit/s.
        bitrate: Option<VideoEncoderBitrate>,

        /// (**default=`5000`**) Maximal interval between keyframes, in milliseconds.
        keyframe_interval_ms: Option<f64>,

        /// Raw FFmpeg encoder options. See [docs](https://ffmpeg.org/ffmpeg-codecs.html) for more.
        ffmpeg_options: Option<HashMap<Arc<str>, Arc<str>>>,
    },
    #[serde(rename = "ffmpeg_vp9")]
    FfmpegVp9 {
        /// Encoding bitrate. If not provided, bitrate is calculated based on resolution and framerate.
        /// For example at 1080p 30 FPS the average bitrate is 5000 kbit/s and max bitrate is 6250 kbit/s.
        bitrate: Option<VideoEncoderBitrate>,

        /// (**default=`5000`**) Maximal interval between keyframes, in milliseconds.
        keyframe_interval_ms: Option<f64>,

        /// (**default=`"yuv420p"`**) Encoder pixel format.
        pixel_format: Option<PixelFormat>,

        /// Raw FFmpeg encoder options. See [docs](https://ffmpeg.org/ffmpeg-codecs.html) for more.
        ffmpeg_options: Option<HashMap<Arc<str>, Arc<str>>>,
    },
    #[serde(rename = "vulkan_h264")]
    VulkanH264 {
        /// Encoding bitrate. If not provided, bitrate is calculated based on resolution and framerate.
        /// For example at 1080p 30 FPS the average bitrate is 5000 kbit/s and max bitrate is 6250 kbit/s.
        bitrate: Option<VideoEncoderBitrate>,

        /// (**default=`5000`**) Interval between keyframes, in milliseconds.
        keyframe_interval_ms: Option<f64>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct OutputMoqClientAudioOptions {
    /// (**default="sum_clip"**) Specifies how audio should be mixed.
    pub mixing_strategy: Option<AudioMixingStrategy>,
    /// Condition for termination of the output stream based on the input streams states. If output includes both audio and video streams, then EOS needs to be sent for every type.
    pub send_eos_when: Option<OutputEndCondition>,
    /// Audio encoder options.
    pub encoder: MoqClientAudioEncoderOptions,
    /// Channels configuration.
    pub channels: Option<AudioChannels>,
    /// Initial audio mixer configuration for output.
    pub initial: AudioScene,
}

#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum MoqClientAudioEncoderOptions {
    Aac {
        /// (**default=`44100`**) Sample rate. Allowed values: [8000, 16000, 24000, 44100, 48000].
        sample_rate: Option<u32>,
    },
    Opus {
        /// (**default=`"voip"`**) Audio output encoder preset.
        preset: Option<OpusEncoderPreset>,

        /// (**default=`48000`**) Sample rate. Allowed values: [8000, 16000, 24000, 48000].
        sample_rate: Option<u32>,
    },
}
