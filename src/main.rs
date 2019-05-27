extern crate clap;
extern crate fibers_rpc;
extern crate frugalos;
extern crate frugalos_config;
extern crate frugalos_segment;
extern crate hostname;
extern crate jemallocator;
extern crate libfrugalos;
#[macro_use]
extern crate slog;
extern crate sloggers;
#[macro_use]
extern crate trackable;

use clap::{App, Arg, ArgMatches, SubCommand};
use libfrugalos::entity::server::Server;
use libfrugalos::time::Seconds;
use sloggers::Build;
use std::env;
use std::net::ToSocketAddrs;
use std::string::ToString;
use std::time::Duration;
use trackable::error::{ErrorKindExt, Failure};

use frugalos::FrugalosConfig;
use frugalos::{Error, ErrorKind, Result};

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

// See https://github.com/frugalos/frugalos/pull/161
#[allow(
    renamed_and_removed_lints,
    clippy::cyclomatic_complexity,
    clippy::cognitive_complexity
)]
fn main() {
    let rpc_server_bind_addr = default_rpc_server_bind_addr();
    let long_version = track_try_unwrap!(make_long_version());
    let matches = App::new("frugalos")
        .version(env!("CARGO_PKG_VERSION"))
        .long_version(long_version.as_str())
        .subcommand(
            SubCommand::with_name("create")
                .arg(server_id_arg())
                .arg(server_addr_arg(&rpc_server_bind_addr))
                .arg(data_dir_arg()),
        )
        .subcommand(
            SubCommand::with_name("join")
                .arg(server_id_arg())
                .arg(server_addr_arg(&rpc_server_bind_addr))
                .arg(contact_server_addr_arg())
                .arg(data_dir_arg()),
        )
        .subcommand(
            SubCommand::with_name("leave")
                .arg(contact_server_addr_arg())
                .arg(data_dir_arg()),
        )
        .subcommand(
            SubCommand::with_name("repair-local-dat")
                .arg(server_id_arg())
                .arg(server_addr_arg(&rpc_server_bind_addr).required(true))
                .arg(server_seqno_arg().required(true))
                .arg(data_dir_arg()),
        )
        .subcommand(
            SubCommand::with_name("start")
                .arg(
                    Arg::with_name("SAMPLING_RATE")
                        .long("sampling-rate")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("EXECUTOR_THREADS")
                        .long("threads")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("HTTP_SERVER_BIND_ADDR")
                        .long("http-server-bind-addr")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("STOP_WAITING_TIME_MILLIS")
                        .long("stop-waiting-time-millis")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("RPC_CONNECT_TIMEOUT_MILLIS")
                        .long("rpc-connect-timeout-millis")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("RPC_WRITE_TIMEOUT_MILLIS")
                        .long("rpc-write-timeout-millis")
                        .takes_value(true),
                )
                .arg(data_dir_arg())
                .arg(put_content_timeout_arg()),
        )
        .subcommand(
            SubCommand::with_name("stop").arg(
                Arg::with_name("RPC_ADDR")
                    .long("rpc-addr")
                    .takes_value(true)
                    .default_value(&rpc_server_bind_addr),
            ),
        )
        .subcommand(
            SubCommand::with_name("take-snapshot").arg(
                Arg::with_name("RPC_ADDR")
                    .long("rpc-addr")
                    .takes_value(true)
                    .default_value(&rpc_server_bind_addr),
            ),
        )
        .arg(
            Arg::with_name("LOGLEVEL")
                .short("l")
                .long("loglevel")
                .takes_value(true)
                .possible_values(&["debug", "info", "warning", "error", "critical"]),
        )
        .arg(
            Arg::with_name("MAX_CONCURRENT_LOGS")
                .long("max_concurrent_logs")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("CONFIG_FILE")
                .long("config-file")
                .takes_value(true),
        )
        .get_matches();

    let (mut config, unknown_fields): (FrugalosConfig, Vec<String>) =
        track_try_unwrap!(track_any_err!(get_frugalos_config(&matches)));

    // Logger
    config.loglevel = matches
        .value_of("LOGLEVEL")
        .map(|v| match v {
            "debug" => sloggers::types::Severity::Debug,
            "info" => sloggers::types::Severity::Info,
            "warning" => sloggers::types::Severity::Warning,
            "error" => sloggers::types::Severity::Error,
            "critical" => sloggers::types::Severity::Critical,
            _ => unreachable!(),
        })
        .unwrap_or(config.loglevel);
    if let Some(v) = matches.value_of("MAX_CONCURRENT_LOGS") {
        config.max_concurrent_logs = track_try_unwrap!(v.parse().map_err(Error::from));
    }
    let logger_builder;
    {
        let maybe_log_file = config.log_file.as_ref().and_then(|p| p.to_str());
        logger_builder = if let Some(filepath) = matches.value_of("LOGFILE").or(maybe_log_file) {
            let mut builder = sloggers::file::FileLoggerBuilder::new(filepath);
            builder.level(config.loglevel);
            builder.channel_size(config.max_concurrent_logs);
            sloggers::LoggerBuilder::File(builder)
        } else {
            let mut builder = sloggers::terminal::TerminalLoggerBuilder::new();
            builder.level(config.loglevel);
            builder.channel_size(config.max_concurrent_logs);
            sloggers::LoggerBuilder::Terminal(builder)
        };
    }

    // SubCommands
    if let Some(matches) = matches.subcommand_matches("create") {
        // CREATE CLUSTER
        let server_id = matches
            .value_of("SERVER_ID")
            .map(ToString::to_string)
            .or_else(hostname::get_hostname)
            .unwrap();
        let server_addr = matches.value_of("SERVER_ADDR").unwrap();
        set_data_dir(&matches, &mut config);

        let mut logger = track_try_unwrap!(logger_builder.build());
        warn_if_there_are_unknown_fields(&mut logger, &unknown_fields);
        let logger = logger.new(o!("server" => format!("{}@{}", server_id, server_addr)));
        let server = Server::new(
            server_id.to_string(),
            track_try_unwrap!(server_addr.parse().map_err(Failure::from_error)),
        );
        debug!(logger, "config: {:?}", config);
        track_try_unwrap!(frugalos_config::cluster::create(
            &logger,
            server,
            config.data_dir
        ));
    } else if let Some(matches) = matches.subcommand_matches("join") {
        // JOIN CLUSTER
        let server_id = matches
            .value_of("SERVER_ID")
            .map(ToString::to_string)
            .or_else(hostname::get_hostname)
            .unwrap();
        let server_addr = matches.value_of("SERVER_ADDR").unwrap();
        let contact_server_addr = matches.value_of("CONTACT_SERVER_ADDR").unwrap();
        set_data_dir(&matches, &mut config);

        let mut logger = track_try_unwrap!(logger_builder.build());
        warn_if_there_are_unknown_fields(&mut logger, &unknown_fields);
        let logger = logger.new(o!("server" => format!("{}@{}", server_id, server_addr)));
        let server = Server::new(
            server_id.to_string(),
            track_try_unwrap!(server_addr.parse().map_err(Failure::from_error)),
        );
        let contact_server =
            track_try_unwrap!(contact_server_addr.parse().map_err(Failure::from_error));
        debug!(logger, "config: {:?}", config);
        track_try_unwrap!(frugalos_config::cluster::join(
            &logger,
            &server,
            config.data_dir,
            contact_server,
        ));
    } else if let Some(matches) = matches.subcommand_matches("leave") {
        // LEAVE CLUSTER
        let contact_server_addr = matches.value_of("CONTACT_SERVER_ADDR").unwrap();
        set_data_dir(&matches, &mut config);

        let contact_server =
            track_try_unwrap!(contact_server_addr.parse().map_err(Failure::from_error));
        let mut logger = track_try_unwrap!(logger_builder.build());
        warn_if_there_are_unknown_fields(&mut logger, &unknown_fields);
        debug!(logger, "config: {:?}", config);
        track_try_unwrap!(frugalos_config::cluster::leave(
            &logger,
            config.data_dir,
            contact_server,
        ));
    } else if let Some(matches) = matches.subcommand_matches("repair-local-dat") {
        let server_id = matches
            .value_of("SERVER_ID")
            .map(ToString::to_string)
            .or_else(hostname::get_hostname)
            .unwrap();
        let server_addr = matches.value_of("SERVER_ADDR").unwrap();
        set_data_dir(&matches, &mut config);

        let mut logger = track_try_unwrap!(logger_builder.build());
        warn_if_there_are_unknown_fields(&mut logger, &unknown_fields);
        let logger = logger.new(o!("server" => format!("{}@{}", server_id, server_addr)));
        let mut server = Server::new(
            server_id.to_string(),
            track_try_unwrap!(server_addr.parse().map_err(Failure::from_error)),
        );
        server.seqno = track_try_unwrap!(get_server_seqno(matches));
        debug!(logger, "config: {:?}", config);
        track_try_unwrap!(frugalos_config::cluster::save_local_server_info(
            config.data_dir,
            server,
        ));
    } else if let Some(matches) = matches.subcommand_matches("start") {
        // START SERVER
        let mut logger = track_try_unwrap!(logger_builder.build());
        warn_if_there_are_unknown_fields(&mut logger, &unknown_fields);
        set_data_dir(&matches, &mut config);
        track_try_unwrap!(track_any_err!(set_daemon_config(
            &matches,
            &mut config.daemon
        )));
        track_try_unwrap!(track_any_err!(set_http_server_config(
            &matches,
            &mut config.http_server
        )));
        track_try_unwrap!(track_any_err!(set_rpc_client_config(
            &matches,
            &mut config.rpc_client
        )));
        track_try_unwrap!(track_any_err!(set_segment_config(
            &matches,
            &mut config.segment
        )));
        let daemon = track_try_unwrap!(frugalos::daemon::FrugalosDaemon::new(
            &logger,
            config.clone()
        ));
        track_try_unwrap!(daemon.run(config.daemon.clone()));
        // NOTE: ログ出力(非同期)用に少し待機
        std::thread::sleep(std::time::Duration::from_millis(100));
        debug!(logger, "config: {:?}", config);
    } else if let Some(matches) = matches.subcommand_matches("stop") {
        // STOP SERVER
        let mut logger = track_try_unwrap!(logger_builder.build());
        warn_if_there_are_unknown_fields(&mut logger, &unknown_fields);
        let mut rpc_addrs = track_try_unwrap!(track_any_err!(matches
            .value_of("RPC_ADDR")
            .unwrap()
            .to_socket_addrs()));
        let rpc_addr = rpc_addrs.nth(0).expect("No available TCP address");
        let logger = logger.new(o!("rpc_addr" => rpc_addr.to_string()));
        track_try_unwrap!(frugalos::daemon::stop(&logger, rpc_addr));

        // NOTE: ログ出力(非同期)用に少し待機
        std::thread::sleep(std::time::Duration::from_millis(100));
        debug!(logger, "config: {:?}", config);
    } else if let Some(matches) = matches.subcommand_matches("take-snapshot") {
        // TAKE SNAPSHOT
        let mut logger = track_try_unwrap!(logger_builder.build());
        warn_if_there_are_unknown_fields(&mut logger, &unknown_fields);
        let mut rpc_addrs = track_try_unwrap!(track_any_err!(matches
            .value_of("RPC_ADDR")
            .unwrap()
            .to_socket_addrs()));
        let rpc_addr = rpc_addrs.nth(0).expect("No available TCP address");
        let logger = logger.new(o!("rpc_addr" => rpc_addr.to_string()));
        track_try_unwrap!(frugalos::daemon::take_snapshot(&logger, rpc_addr));

        // NOTE: ログ出力(非同期)用に少し待機
        std::thread::sleep(std::time::Duration::from_millis(100));
        debug!(logger, "config: {:?}", config);
    } else {
        println!("Usage: {}", matches.usage());
        std::process::exit(1);
    }
}

fn server_id_arg<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name("SERVER_ID")
        .help("Sets the identifier of this server (the default is the hostname of this machine)")
        .long("id")
        .takes_value(true)
}

// NOTE: The address of RPC server
fn server_addr_arg<'a, 'b>(default_value: &'a str) -> Arg<'a, 'b> {
    Arg::with_name("SERVER_ADDR")
        .long("addr")
        .alias("rpc-addr")
        .takes_value(true)
        .default_value(default_value)
}

fn server_seqno_arg<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name("SERVER_SEQNO")
        .help("seqno of this server")
        .long("seqno")
        .takes_value(true)
}

fn contact_server_addr_arg<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name("CONTACT_SERVER_ADDR")
        .long("contact-server")
        .takes_value(true)
        .required(true)
}

fn data_dir_arg<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name("DATA_DIR")
        .help(
            "Sets the data directory of this server \
             (the default is the value of FRUGALOS_DATA_DIR environment variable)",
        )
        .long("data-dir")
        .takes_value(true)
}

fn put_content_timeout_arg<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name("PUT_CONTENT_TIMEOUT")
        .help("Sets timeout in seconds on putting a content.")
        .long("put-content-timeout")
        .takes_value(true)
}

fn default_rpc_server_bind_addr() -> String {
    "127.0.0.1:14278".to_owned()
}

fn get_server_seqno(matches: &ArgMatches) -> Result<u32> {
    matches
        .value_of("SERVER_SEQNO")
        .map(|v| v.parse::<u32>().map_err(|e| track!(Error::from(e))))
        .unwrap_or_else(|| {
            Err(Error::from(
                ErrorKind::InvalidInput.cause("server seqno must be specified"),
            ))
        })
}

fn set_data_dir(matches: &ArgMatches, config: &mut FrugalosConfig) {
    if let Some(value) = matches
        .value_of("DATA_DIR")
        .map(ToString::to_string)
        .or_else(|| env::var("FRUGALOS_DATA_DIR").ok())
    {
        config.data_dir = value;
    }

    if config.data_dir.is_empty() {
        println!(
            "[ERROR] Must set one of the `data-dir` argument, the `FRUGALOS_DATA_DIR` environment variable or the `frugalos.data_dir` key of a configuration file"
        );
        std::process::exit(1);
    }
}

/// Gets `FrugalosConfig`.
fn get_frugalos_config(matches: &ArgMatches) -> Result<(FrugalosConfig, Vec<String>)> {
    matches.value_of("CONFIG_FILE").map_or_else(
        || Ok((FrugalosConfig::default(), Vec::new())),
        |v| FrugalosConfig::from_yaml(v).map_err(|e| track!(e)),
    )
}

/// Sets configurations for frugalos daemon.
fn set_daemon_config(
    matches: &ArgMatches,
    config: &mut frugalos::FrugalosDaemonConfig,
) -> Result<()> {
    if let Some(threads) = matches.value_of("EXECUTOR_THREADS") {
        config.executor_threads = threads.parse().map_err(|e| track!(Error::from(e)))?;
    }
    if let Some(v) = matches.value_of("SAMPLING_RATE") {
        config.sampling_rate = v.parse().map_err(|e| track!(Error::from(e)))?;
    }
    if let Some(v) = matches.value_of("STOP_WAITING_TIME_MILLIS") {
        config.stop_waiting_time = v
            .parse::<u64>()
            .map(Duration::from_millis)
            .map_err(|e| track!(Error::from(e)))?;
    }
    Ok(())
}

/// Sets configurations for a HTTP server.
fn set_http_server_config(
    matches: &ArgMatches,
    config: &mut frugalos::FrugalosHttpServerConfig,
) -> Result<()> {
    if let Some(v) = matches.value_of("HTTP_SERVER_BIND_ADDR") {
        config.bind_addr = v.parse().map_err(|e| track!(Error::from(e)))?;
    }
    Ok(())
}

/// Sets configurations for an RPC client.
fn set_rpc_client_config(
    matches: &ArgMatches,
    config: &mut frugalos::FrugalosRpcClientConfig,
) -> Result<()> {
    if let Some(v) = matches.value_of("RPC_CONNECT_TIMEOUT_MILLIS") {
        config.tcp_connect_timeout = v
            .parse::<u64>()
            .map(Duration::from_millis)
            .map_err(|e| track!(Error::from(e)))?;
    }
    if let Some(v) = matches.value_of("RPC_WRITE_TIMEOUT_MILLIS") {
        config.tcp_write_timeout = v
            .parse::<u64>()
            .map(Duration::from_millis)
            .map_err(|e| track!(Error::from(e)))?;
    }
    Ok(())
}

/// Sets configurations for frugalos segment.
fn set_segment_config(
    matches: &ArgMatches,
    config: &mut frugalos_segment::FrugalosSegmentConfig,
) -> Result<()> {
    if let Some(v) = matches.value_of("PUT_CONTENT_TIMEOUT") {
        config.mds_client.put_content_timeout = v
            .parse::<u64>()
            .map(Seconds)
            .map_err(|e| track!(Error::from(e)))?;
    }
    Ok(())
}

fn warn_if_there_are_unknown_fields(logger: &mut slog::Logger, unknown_fields: &[String]) {
    if !unknown_fields.is_empty() {
        warn!(
            logger,
            "The following unknown fields were passed:\n{:?}", unknown_fields
        );
    }
}

fn make_long_version() -> Result<String> {
    use frugalos::build_information::*;
    use std::fmt::Write;

    let mut s = String::new();
    track!(writeln!(&mut s, env!("CARGO_PKG_VERSION")).map_err(Error::from))?;
    track!(writeln!(&mut s, "build-mode: {}", BUILD_PROFILE).map_err(Error::from))?;
    track!(writeln!(&mut s, "rustc-version: {}", BUILD_VERSION).map_err(Error::from))?;
    Ok(s.trim().to_owned())
}
