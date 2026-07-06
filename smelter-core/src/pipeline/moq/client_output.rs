use hang::moq_net::{Origin, OriginProducer};
use moq_native::ClientConfig;
use smelter_render::OutputId;
use std::sync::Arc;
use tracing::info;
use url::Url;

use crate::{
    OutputProtocolKind, PipelineCtx, Ref,
    error::OutputInitError,
    pipeline::{
        encoder::{
            encoder_thread_audio::AudioEncoderThreadHandle,
            encoder_thread_video::VideoEncoderThreadHandle,
        },
        moq::MoqSession,
        output::{Output, OutputAudio, OutputVideo},
    },
    prelude::{MoqClientError, MoqClientOutputOptions},
};

pub struct MoqClientOutput {
    video: Option<VideoEncoderThreadHandle>,
    audio: Option<AudioEncoderThreadHandle>,

    session: Option<MoqSession>,
}

impl MoqClientOutput {
    pub fn new(
        ctx: Arc<PipelineCtx>,
        output_ref: Ref<OutputId>,
        options: MoqClientOutputOptions,
    ) -> Result<Self, OutputInitError> {
        todo!()
    }
}

impl MoqClientOutput {
    fn connect(
        &mut self,
        ctx: &Arc<PipelineCtx>,
        url: &str,
        disable_tls_verification: bool,
    ) -> Result<OriginProducer, MoqClientError> {
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
        self.session = Some(session);
        Ok(producer)
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
