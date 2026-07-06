use std::sync::Arc;

use crossbeam_channel::{Receiver, Select, bounded};
use hang::{
    catalog::{
        AAC as MoqAAC, AudioCodec as HangAudioCodec, AudioConfig as MoqAudioConfig,
        Container as CatalogContainer, H264 as MoqH264, VideoCodec as HangVideoCodec,
        VideoConfig as MoqVideoConfig,
    },
    moq_net::{Broadcast, BroadcastProducer, Origin, OriginProducer, Track},
};
use moq_mux::{
    catalog::Producer as CatalogProducer,
    container::{
        Frame as MediaFrame, Producer as ContainerProducer, Timestamp as MediaTimestamp, loc,
    },
};
use moq_native::ClientConfig;
use smelter_render::OutputId;
use tracing::{debug, info, warn};
use url::Url;

use crate::{
    OutputProtocolKind, PipelineCtx, Ref,
    error::OutputInitError,
    event::Event,
    pipeline::{
        encoder::{
            encoder_thread_audio::{
                AudioEncoderThread, AudioEncoderThreadHandle, AudioEncoderThreadOptions,
            },
            encoder_thread_video::{
                VideoEncoderThread, VideoEncoderThreadHandle, VideoEncoderThreadOptions,
            },
            fdk_aac::FdkAacEncoder,
            ffmpeg_h264::FfmpegH264Encoder,
            libopus::OpusEncoder,
            vulkan_h264::VulkanH264Encoder,
        },
        moq::MoqSession,
        output::{Output, OutputAudio, OutputVideo},
    },
    utils::InitializableThread,
};

use crate::prelude::*;

/// Track names used in the published catalog and on the wire. The catalog
/// rendition key and the moq-lite track name must match so consumers can
/// subscribe by the name they discover in the catalog.
const VIDEO_TRACK: &str = "video";
const AUDIO_TRACK: &str = "audio";

/// A single encoded track ready to be muxed: the container producer that owns
/// the moq-lite track, plus the channel that delivers encoded chunks from the
/// encoder thread.
struct TrackMuxer {
    producer: ContainerProducer<loc::Wire>,
    chunks_receiver: Receiver<EncodedOutputEvent>,
}

pub struct MoqClientOutput {
    video: Option<VideoEncoderThreadHandle>,
    audio: Option<AudioEncoderThreadHandle>,

    // Kept alive for the duration of the output. Dropping the session closes
    // the QUIC connection; dropping the origin/broadcast un-announces the
    // broadcast from the relay.
    _session: MoqSession,
    _origin: OriginProducer,
    _broadcast: BroadcastProducer,
    _catalog: CatalogProducer,
}

impl MoqClientOutput {
    pub fn new(
        ctx: Arc<PipelineCtx>,
        output_ref: Ref<OutputId>,
        options: MoqClientOutputOptions,
    ) -> Result<Self, OutputInitError> {
        // Producing (creating tracks, publishing the catalog, writing groups)
        // touches moq-net internals that expect a Tokio context.
        let _rt_guard = ctx.tokio_rt.enter();

        // 1. Spawn encoder threads for the configured tracks.
        let video = match &options.video {
            Some(opts) => Some(Self::init_video_encoder(&ctx, &output_ref, opts.clone())?),
            None => None,
        };
        let audio = match &options.audio {
            Some(opts) => Some(Self::init_audio_encoder(&ctx, &output_ref, opts.clone())?),
            None => None,
        };

        // TODO: emit `StatsEvent::NewOutput` once MoQ client output stats are
        // implemented (see stats/output/mod.rs). Emitting it now would hit an
        // `unimplemented!()` arm in `OutputStatsState::new`.

        // 2. Connect to the relay. The client publishes whatever we announce
        //    through `origin`.
        let (session, origin) = Self::connect(
            &ctx,
            &options.endpoint_url,
            options.disable_tls_verification,
        )?;

        // 3. Build the broadcast: a catalog track plus one media track per
        //    configured encoder.
        let mut broadcast = Broadcast::new().produce();
        let mut catalog = CatalogProducer::new(&mut broadcast)
            .map_err(|err| MoqClientError::BroadcastSetupFailed(err.into()))?;

        let video_muxer = match &video {
            Some((handle, chunks_receiver)) => {
                let config = video_catalog_config(handle);
                let track = broadcast
                    .create_track(Track::new(VIDEO_TRACK))
                    .map_err(|err| MoqClientError::BroadcastSetupFailed(err.into()))?;
                catalog
                    .lock()
                    .video
                    .insert(VIDEO_TRACK, config)
                    .map_err(|err| MoqClientError::BroadcastSetupFailed(err.into()))?;
                // Live ingest can start mid-GOP; drop any deltas before the
                // first keyframe rather than treating them as a violation.
                let producer = ContainerProducer::new(track, loc::Wire).with_lenient_start();
                Some(TrackMuxer {
                    producer,
                    chunks_receiver: chunks_receiver.clone(),
                })
            }
            None => None,
        };

        let audio_muxer = match (&audio, &options.audio) {
            (Some((handle, chunks_receiver)), Some(opts)) => {
                let config = audio_catalog_config(handle, opts);
                let track = broadcast
                    .create_track(Track::new(AUDIO_TRACK))
                    .map_err(|err| MoqClientError::BroadcastSetupFailed(err.into()))?;
                catalog
                    .lock()
                    .audio
                    .insert(AUDIO_TRACK, config)
                    .map_err(|err| MoqClientError::BroadcastSetupFailed(err.into()))?;
                let producer = ContainerProducer::new(track, loc::Wire);
                Some(TrackMuxer {
                    producer,
                    chunks_receiver: chunks_receiver.clone(),
                })
            }
            _ => None,
        };

        // 4. Announce the broadcast to the relay.
        if !origin.publish_broadcast(options.broadcast_path.as_ref(), broadcast.consume()) {
            warn!(
                broadcast_path = %options.broadcast_path,
                "MoQ broadcast path already announced on this session."
            );
        }

        // 5. Spawn the muxer thread that drains the encoders into the tracks.
        Self::spawn_muxer_thread(ctx.clone(), output_ref.clone(), video_muxer, audio_muxer);

        info!(broadcast_path = %options.broadcast_path, "MoQ client output started");

        Ok(Self {
            video: video.map(|(handle, _)| handle),
            audio: audio.map(|(handle, _)| handle),
            _session: session,
            _origin: origin,
            _broadcast: broadcast,
            _catalog: catalog,
        })
    }

    fn connect(
        ctx: &Arc<PipelineCtx>,
        url: &str,
        disable_tls_verification: bool,
    ) -> Result<(MoqSession, OriginProducer), MoqClientError> {
        let url = Url::parse(url).map_err(|err| MoqClientError::InvalidUrl(Arc::from(url), err))?;

        if url.scheme() != "https" {
            return Err(MoqClientError::InvalidScheme(url.scheme().to_string()));
        }

        let mut config = ClientConfig::default();
        config.tls.disable_verify = Some(disable_tls_verification);
        let client = config.init().map_err(MoqClientError::ClientInitFailed)?;

        // We locally produce the broadcasts and hand the consumer side to the
        // client, which announces and publishes them to the relay.
        let producer = Origin::random().produce();
        let consumer = producer.consume();
        let client = client.with_publish(consumer);

        let session = ctx
            .tokio_rt
            .block_on(client.connect(url))
            .map_err(MoqClientError::ConnectFailed)?;
        let session = MoqSession::new(session, ctx.tokio_rt.clone());
        info!(moq_version = ?session.version(), "MoQ client session established");
        Ok((session, producer))
    }

    fn init_video_encoder(
        ctx: &Arc<PipelineCtx>,
        output_ref: &Ref<OutputId>,
        options: VideoEncoderOptions,
    ) -> Result<(VideoEncoderThreadHandle, Receiver<EncodedOutputEvent>), OutputInitError> {
        let (chunks_sender, chunks_receiver) = bounded(1000);
        let handle = match &options {
            VideoEncoderOptions::FfmpegH264(options) => {
                VideoEncoderThread::<FfmpegH264Encoder>::spawn(
                    output_ref.clone(),
                    VideoEncoderThreadOptions {
                        ctx: ctx.clone(),
                        encoder_options: options.clone(),
                        chunks_sender,
                    },
                )?
            }
            VideoEncoderOptions::VulkanH264(options) => {
                if !ctx.graphics_context.has_vulkan_encoder_support() {
                    return Err(OutputInitError::EncoderError(
                        EncoderInitError::VulkanContextRequiredForVulkanEncoder,
                    ));
                }
                VideoEncoderThread::<VulkanH264Encoder>::spawn(
                    output_ref.clone(),
                    VideoEncoderThreadOptions {
                        ctx: ctx.clone(),
                        encoder_options: options.clone(),
                        chunks_sender,
                    },
                )?
            }
            VideoEncoderOptions::FfmpegVp8(_) => {
                return Err(MoqClientError::UnsupportedCodec("VP8").into());
            }
            VideoEncoderOptions::FfmpegVp9(_) => {
                return Err(MoqClientError::UnsupportedCodec("VP9").into());
            }
        };
        Ok((handle, chunks_receiver))
    }

    fn init_audio_encoder(
        ctx: &Arc<PipelineCtx>,
        output_ref: &Ref<OutputId>,
        options: AudioEncoderOptions,
    ) -> Result<(AudioEncoderThreadHandle, Receiver<EncodedOutputEvent>), OutputInitError> {
        let (chunks_sender, chunks_receiver) = bounded(1000);
        let handle = match options {
            AudioEncoderOptions::FdkAac(options) => AudioEncoderThread::<FdkAacEncoder>::spawn(
                output_ref.clone(),
                AudioEncoderThreadOptions {
                    ctx: ctx.clone(),
                    encoder_options: options,
                    chunks_sender,
                },
            )?,
            AudioEncoderOptions::Opus(options) => AudioEncoderThread::<OpusEncoder>::spawn(
                output_ref.clone(),
                AudioEncoderThreadOptions {
                    ctx: ctx.clone(),
                    encoder_options: options,
                    chunks_sender,
                },
            )?,
        };
        Ok((handle, chunks_receiver))
    }

    fn spawn_muxer_thread(
        ctx: Arc<PipelineCtx>,
        output_ref: Ref<OutputId>,
        video: Option<TrackMuxer>,
        audio: Option<TrackMuxer>,
    ) {
        std::thread::Builder::new()
            .name(format!("MoQ muxer thread for output {output_ref}"))
            .spawn(move || {
                let _span =
                    tracing::info_span!("MoQ muxer", output_id = output_ref.to_string()).entered();
                let _rt_guard = ctx.tokio_rt.enter();

                run_muxer(video, audio);

                ctx.event_emitter
                    .emit(Event::OutputDone(output_ref.id().clone()));
                debug!("Closing MoQ muxer thread.");
            })
            .unwrap();
    }
}

impl Output for MoqClientOutput {
    fn video(&self) -> Option<OutputVideo<'_>> {
        self.video.as_ref().map(|video| OutputVideo {
            resolution: video.config.resolution,
            frame_format: video.config.output_format,
            frame_sender: &video.frame_sender,
            keyframe_request_sender: &video.keyframe_request_sender,
        })
    }

    fn audio(&self) -> Option<OutputAudio<'_>> {
        self.audio.as_ref().map(|audio| OutputAudio {
            samples_batch_sender: &audio.sample_batch_sender,
        })
    }

    fn kind(&self) -> OutputProtocolKind {
        OutputProtocolKind::MoqClient
    }
}

/// Drain both encoder channels into their tracks until each reaches EOS.
///
/// Video group boundaries are driven by the encoder's keyframes. Audio frames
/// (Opus/AAC) are each independently decodable, so every frame is marked as a
/// keyframe, giving each its own group.
fn run_muxer(video: Option<TrackMuxer>, audio: Option<TrackMuxer>) {
    // Split the receivers (registered with `Select`, borrowed immutably) from
    // the producers (written to, borrowed mutably) so the two don't alias.
    let (mut video_rx, mut video_prod) = split_muxer(video);
    let (mut audio_rx, mut audio_prod) = split_muxer(audio);

    loop {
        if video_rx.is_none() && audio_rx.is_none() {
            break;
        }

        let mut sel = Select::new();
        let video_idx = video_rx.as_ref().map(|rx| sel.recv(rx));
        let audio_idx = audio_rx.as_ref().map(|rx| sel.recv(rx));

        let oper = sel.select();
        let idx = oper.index();

        if Some(idx) == video_idx {
            match oper.recv(video_rx.as_ref().unwrap()) {
                Ok(EncodedOutputEvent::Data(chunk)) => {
                    write_chunk(video_prod.as_mut().unwrap(), chunk, StatsTrackKind::Video)
                }
                _ => {
                    if let Some(mut prod) = video_prod.take() {
                        let _ = prod.finish();
                    }
                    video_rx = None;
                }
            }
        } else if Some(idx) == audio_idx {
            match oper.recv(audio_rx.as_ref().unwrap()) {
                Ok(EncodedOutputEvent::Data(chunk)) => {
                    write_chunk(audio_prod.as_mut().unwrap(), chunk, StatsTrackKind::Audio)
                }
                _ => {
                    if let Some(mut prod) = audio_prod.take() {
                        let _ = prod.finish();
                    }
                    audio_rx = None;
                }
            }
        }
    }
}

fn split_muxer(
    muxer: Option<TrackMuxer>,
) -> (
    Option<Receiver<EncodedOutputEvent>>,
    Option<ContainerProducer<loc::Wire>>,
) {
    match muxer {
        Some(TrackMuxer {
            producer,
            chunks_receiver,
        }) => (Some(chunks_receiver), Some(producer)),
        None => (None, None),
    }
}

fn write_chunk(
    producer: &mut ContainerProducer<loc::Wire>,
    chunk: EncodedOutputChunk,
    track_kind: StatsTrackKind,
) {
    // Audio frames are always independently decodable; treat them as keyframes
    // so each starts its own group.
    let keyframe = match track_kind {
        StatsTrackKind::Video => chunk.is_keyframe,
        StatsTrackKind::Audio => true,
    };
    let Ok(timestamp) = MediaTimestamp::try_from(chunk.pts) else {
        warn!(pts = ?chunk.pts, "Dropping MoQ frame with out-of-range timestamp.");
        return;
    };
    let frame = MediaFrame {
        timestamp,
        payload: chunk.data,
        keyframe,
    };
    if let Err(err) = producer.write(frame) {
        warn!(?track_kind, "Failed to write MoQ frame: {err}");
    }
}

fn video_catalog_config(handle: &VideoEncoderThreadHandle) -> MoqVideoConfig {
    let resolution = handle.config.resolution;
    let description = handle.encoder_context();

    // Only H264 is supported. The avcC record carries the profile, constraint
    // flags and level in bytes 1..=3; fall back to zeros if it's missing.
    let hang_codec = HangVideoCodec::H264(MoqH264 {
        inline: false,
        profile: description
            .as_ref()
            .and_then(|d| d.get(1).copied())
            .unwrap_or(0),
        constraints: description
            .as_ref()
            .and_then(|d| d.get(2).copied())
            .unwrap_or(0),
        level: description
            .as_ref()
            .and_then(|d| d.get(3).copied())
            .unwrap_or(0),
    });

    let mut config = MoqVideoConfig::new(hang_codec);
    config.container = CatalogContainer::Loc;
    config.description = description;
    config.coded_width = Some(resolution.width as u32);
    config.coded_height = Some(resolution.height as u32);
    config.optimize_for_latency = Some(true);
    config
}

fn audio_catalog_config(
    handle: &AudioEncoderThreadHandle,
    options: &AudioEncoderOptions,
) -> MoqAudioConfig {
    let sample_rate = options.sample_rate();
    let channel_count = match options.channels() {
        AudioChannels::Mono => 1,
        AudioChannels::Stereo => 2,
    };
    let description = handle.encoder_context();

    let hang_codec = match options {
        AudioEncoderOptions::FdkAac(_) => {
            // AudioSpecificConfig: the top 5 bits of the first byte are the
            // AudioObjectType, which maps to the AAC profile.
            let profile = description
                .as_ref()
                .and_then(|b| b.first())
                .map_or(2, |b| b >> 3);
            HangAudioCodec::AAC(MoqAAC { profile })
        }
        AudioEncoderOptions::Opus(_) => HangAudioCodec::Opus,
    };

    let mut config = MoqAudioConfig::new(hang_codec, sample_rate, channel_count);
    config.container = CatalogContainer::Loc;
    config.description = description;
    config
}
