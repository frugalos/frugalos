extern crate clap;
extern crate frugalos;
extern crate frugalos_config;
extern crate frugalos_segment;
extern crate hostname;
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
use std::net::{SocketAddr, ToSocketAddrs};
use trackable::error::Failure;

use frugalos::{Error, Result};
use frugalos_segment::config::MdsClientConfig;

#[cfg_attr(feature = "cargo-clippy", allow(cyclomatic_complexity))]
fn main() {
    let matches = App::new("frugalos")
        .version(env!("CARGO_PKG_VERSION"))
        .subcommand(
            SubCommand::with_name("create")
                .arg(server_id_arg())
                .arg(server_addr_arg())
                .arg(data_dir_arg()),
        ).subcommand(
            SubCommand::with_name("join")
                .arg(server_id_arg())
                .arg(server_addr_arg())
                .arg(contact_server_addr_arg())
                .arg(data_dir_arg()),
        ).subcommand(
            SubCommand::with_name("leave")
                .arg(contact_server_addr_arg())
                .arg(data_dir_arg()),
        ).subcommand(
            SubCommand::with_name("start")
                .arg(
                    Arg::with_name("SAMPLING_RATE")
                        .long("sampling-rate")
                        .takes_value(true)
                        .default_value("0.001"),
                ).arg(
                    Arg::with_name("EXECUTOR_THREADS")
                        .long("threads")
                        .takes_value(true),
                ).arg(
                    Arg::with_name("HTTP_SERVER_BIND_ADDR")
                        .long("http-server-bind-addr")
                        .takes_value(true)
                        .default_value("0.0.0.0:3000"),
                ).arg(data_dir_arg())
                .arg(default_put_content_timeout_arg())
                .arg(minimum_put_content_timeout_arg()),
        ).subcommand(
            SubCommand::with_name("stop").arg(
                Arg::with_name("RPC_ADDR")
                    .long("rpc-addr")
                    .takes_value(true)
                    .default_value("127.0.0.1:14278"),
            ),
        ).subcommand(
            SubCommand::with_name("take-snapshot").arg(
                Arg::with_name("RPC_ADDR")
                    .long("rpc-addr")
                    .takes_value(true)
                    .default_value("127.0.0.1:14278"),
            ),
        ).arg(
            Arg::with_name("LOGLEVEL")
                .short("l")
                .long("loglevel")
                .takes_value(true)
                .possible_values(&["debug", "info", "warning"])
                .default_value("info"),
        ).arg(
            Arg::with_name("MAX_CONCURRENT_LOGS")
                .long("max_concurrent_logs")
                .takes_value(true)
                .default_value("4096"),
        ).get_matches();

    // Logger
    let loglevel = match matches.value_of("LOGLEVEL").unwrap() {
        "debug" => sloggers::types::Severity::Debug,
        "info" => sloggers::types::Severity::Info,
        "warning" => sloggers::types::Severity::Warning,
        _ => unreachable!(),
    };
    let max_concurrent_logs = track_try_unwrap!(
        matches
            .value_of("MAX_CONCURRENT_LOGS")
            .unwrap()
            .parse()
            .map_err(Failure::from_error)
    );
    let logger_builder = if let Some(filepath) = matches.value_of("LOGFILE") {
        let mut builder = sloggers::file::FileLoggerBuilder::new(filepath);
        builder.level(loglevel);
        builder.channel_size(max_concurrent_logs);
        sloggers::LoggerBuilder::File(builder)
    } else {
        let mut builder = sloggers::terminal::TerminalLoggerBuilder::new();
        builder.level(loglevel);
        builder.channel_size(max_concurrent_logs);
        sloggers::LoggerBuilder::Terminal(builder)
    };

    // SubCommands
    if let Some(matches) = matches.subcommand_matches("create") {
        // CREATE CLUSTER
        let server_id = matches
            .value_of("SERVER_ID")
            .map(|v| v.to_string())
            .or_else(hostname::get_hostname)
            .unwrap();
        let server_addr = matches.value_of("SERVER_ADDR").unwrap();
        let data_dir = get_data_dir(&matches);

        let logger = track_try_unwrap!(logger_builder.build());
        let logger = logger.new(o!("server" => format!("{}@{}", server_id, server_addr)));
        let server = Server::new(
            server_id.to_string(),
            track_try_unwrap!(server_addr.parse().map_err(Failure::from_error)),
        );
        track_try_unwrap!(frugalos_config::cluster::create(&logger, server, data_dir));
    } else if let Some(matches) = matches.subcommand_matches("join") {
        // JOIN CLUSTER
        let server_id = matches
            .value_of("SERVER_ID")
            .map(|v| v.to_string())
            .or_else(hostname::get_hostname)
            .unwrap();
        let server_addr = matches.value_of("SERVER_ADDR").unwrap();
        let contact_server_addr = matches.value_of("CONTACT_SERVER_ADDR").unwrap();
        let data_dir = get_data_dir(&matches);

        let logger = track_try_unwrap!(logger_builder.build());
        let logger = logger.new(o!("server" => format!("{}@{}", server_id, server_addr)));
        let server = Server::new(
            server_id.to_string(),
            track_try_unwrap!(server_addr.parse().map_err(Failure::from_error)),
        );
        let contact_server =
            track_try_unwrap!(contact_server_addr.parse().map_err(Failure::from_error));
        track_try_unwrap!(frugalos_config::cluster::join(
            &logger,
            &server,
            data_dir,
            contact_server,
        ));
    } else if let Some(matches) = matches.subcommand_matches("leave") {
        // LEAVE CLUSTER
        let contact_server_addr = matches.value_of("CONTACT_SERVER_ADDR").unwrap();
        let data_dir = get_data_dir(&matches);

        let contact_server =
            track_try_unwrap!(contact_server_addr.parse().map_err(Failure::from_error));
        let logger = track_try_unwrap!(logger_builder.build());
        track_try_unwrap!(frugalos_config::cluster::leave(
            &logger,
            data_dir,
            contact_server,
        ));
    } else if let Some(matches) = matches.subcommand_matches("start") {
        // START SERVER
        let logger = track_try_unwrap!(logger_builder.build());
        let mut daemon = frugalos::daemon::FrugalosDaemonBuilder::new(logger);

        let data_dir = get_data_dir(&matches);
        let http_addr: SocketAddr = track_try_unwrap!(track_any_err!(
            matches.value_of("HTTP_SERVER_BIND_ADDR").unwrap().parse()
        ));
        let sampling_rate: f64 = track_try_unwrap!(track_any_err!(
            matches.value_of("SAMPLING_RATE").unwrap().parse()
        ));
        daemon.sampling_rate = sampling_rate;
        daemon.mds_client_config =
            track_try_unwrap!(track_any_err!(get_mds_client_config(&matches)));

        if let Some(threads) = matches.value_of("EXECUTOR_THREADS") {
            let threads: usize = track_try_unwrap!(track_any_err!(threads.parse()));
            daemon.executor_threads = threads;
        }

        let daemon = track_try_unwrap!(daemon.finish(data_dir, http_addr,));
        track_try_unwrap!(daemon.run());

        // NOTE: ログ出力(非同期)用に少し待機
        std::thread::sleep(std::time::Duration::from_millis(100));
    } else if let Some(matches) = matches.subcommand_matches("stop") {
        // STOP SERVER
        let logger = track_try_unwrap!(logger_builder.build());
        let mut rpc_addrs = track_try_unwrap!(track_any_err!(
            matches.value_of("RPC_ADDR").unwrap().to_socket_addrs()
        ));
        let rpc_addr = rpc_addrs.nth(0).expect("No available TCP address");
        let logger = logger.new(o!("rpc_addr" => rpc_addr.to_string()));
        track_try_unwrap!(frugalos::daemon::stop(&logger, rpc_addr));

        // NOTE: ログ出力(非同期)用に少し待機
        std::thread::sleep(std::time::Duration::from_millis(100));
    } else if let Some(matches) = matches.subcommand_matches("take-snapshot") {
        // TAKE SNAPSHOT
        let logger = track_try_unwrap!(logger_builder.build());
        let mut rpc_addrs = track_try_unwrap!(track_any_err!(
            matches.value_of("RPC_ADDR").unwrap().to_socket_addrs()
        ));
        let rpc_addr = rpc_addrs.nth(0).expect("No available TCP address");
        let logger = logger.new(o!("rpc_addr" => rpc_addr.to_string()));
        track_try_unwrap!(frugalos::daemon::take_snapshot(&logger, rpc_addr));

        // NOTE: ログ出力(非同期)用に少し待機
        std::thread::sleep(std::time::Duration::from_millis(100));
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
fn server_addr_arg<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name("SERVER_ADDR")
        .long("addr")
        .alias("rpc-addr")
        .takes_value(true)
        .default_value("127.0.0.1:14278")
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
        ).long("data-dir")
        .takes_value(true)
}

fn default_put_content_timeout_arg<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name("DEFAULT_PUT_CONTENT_TIMEOUT")
        .help("Sets the default timeout in seconds on putting a content.")
        .long("default-put-content-timeout")
        .takes_value(true)
        .default_value("60")
}

fn minimum_put_content_timeout_arg<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name("MINIMUM_PUT_CONTENT_TIMEOUT")
        .help("Sets the minimum timeout in seconds on putting a content.")
        .long("minimum-put-content-timeout")
        .takes_value(true)
        .default_value("60")
}

fn get_data_dir(matches: &ArgMatches) -> String {
    if let Some(value) = matches
        .value_of("DATA_DIR")
        .map(|v| v.to_string())
        .or_else(|| env::var("FRUGALOS_DATA_DIR").ok())
    {
        value
    } else {
        println!(
            "[ERROR] Must set either the `data-dir` argument or the `FRUGALOS_DATA_DIR` environment variable"
        );
        std::process::exit(1);
    }
}

/// Gets `MdsClientConfig` from CLI arguments.
fn get_mds_client_config(matches: &ArgMatches) -> Result<MdsClientConfig> {
    let mut config = MdsClientConfig::default();
    config.default_put_content_timeout = matches
        .value_of("DEFAULT_PUT_CONTENT_TIMEOUT")
        .map_or_else(
            || Ok(config.default_put_content_timeout),
            |v| {
                v.parse::<u64>()
                    .map(Seconds)
                    .map_err(|e| track!(Error::from(e)))
            },
        )?;
    config.minimum_put_content_timeout = matches
        .value_of("MINIMUM_PUT_CONTENT_TIMEOUT")
        .map_or_else(
            || Ok(config.minimum_put_content_timeout),
            |v| {
                v.parse::<u64>()
                    .map(Seconds)
                    .map_err(|e| track!(Error::from(e)))
            },
        )?;
    Ok(config)
}
