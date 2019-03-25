use bytecodec::json_codec::JsonEncoder;
use bytecodec::null::NullDecoder;
use fibers_http_server::{HandleRequest, Reply, Req, ServerBuilder as HttpServerBuilder, Status};
use futures;
use httpcodec::{BodyDecoder, BodyEncoder};
use slog::Logger;
use std::str;

use http::{make_json_response, HttpResult};
use {FrugalosConfig, Result};

#[derive(Clone)]
pub struct ParameterServer {
    logger: Logger,
    config: FrugalosConfig,
}

impl ParameterServer {
    pub fn new(logger: Logger, config: FrugalosConfig) -> Self {
        info!(
            logger,
            "ParameterServer::new with the following parameters\n{:?}", config
        );
        ParameterServer { logger, config }
    }
    pub fn register(self, builder: &mut HttpServerBuilder) -> Result<()> {
        track!(builder.add_handler(GetParameters(self.clone())))?;
        Ok(())
    }
}

struct GetParameters(ParameterServer);
impl HandleRequest for GetParameters {
    const METHOD: &'static str = "GET";
    const PATH: &'static str = "/v1/parameters";

    type ReqBody = ();
    type ResBody = HttpResult<FrugalosConfig>;
    type Decoder = BodyDecoder<NullDecoder>;
    type Encoder = BodyEncoder<JsonEncoder<Self::ResBody>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, _req: Req<Self::ReqBody>) -> Self::Reply {
        let response = make_json_response(Status::Ok, Ok(self.0.config.clone()));
        Box::new(futures::finished(response))
    }
}
