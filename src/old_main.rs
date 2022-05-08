use serum_iq::market::load_event_queue;
use std::io::Write;
use std::str::FromStr;
use std::thread;
use std::time::Duration;

use clap::{App, Arg};
use log::info;
use log::{LevelFilter, Record};
use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;

use chrono::prelude::*;
use env_logger::fmt::Formatter;
use env_logger::Builder;
use solana_sdk::pubkey::Pubkey;

pub fn setup_logger(log_thread: bool, rust_log: Option<&str>) {
    let output_format = move |formatter: &mut Formatter, record: &Record| {
        let thread_name = if log_thread {
            format!("(t: {}) ", thread::current().name().unwrap_or("unknown"))
        } else {
            "".to_string()
        };

        let local_time: DateTime<Local> = Local::now();
        let time_str = local_time.format("%H:%M:%S%.3f").to_string();
        write!(
            formatter,
            "{} {}{} - {} - {}\n",
            time_str,
            thread_name,
            record.level(),
            record.target(),
            record.args()
        )
    };

    let mut builder = Builder::new();
    builder
        .format(output_format)
        .filter(None, LevelFilter::Info);

    rust_log.map(|conf| builder.parse_filters(conf));

    builder.init();
}

async fn produce(brokers: &str, topic_name: &str) {
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    // This loop is non blocking: all messages will be sent one after the other, without waiting
    // for the results.
    let futures = (0..5)
        .map(|i| async move {
            // The send operation on the topic returns a future, which will be
            // completed once the result or failure from Kafka is received.
            let delivery_status = producer
                .send(
                    FutureRecord::to(topic_name)
                        .payload(&format!("Message {}", i))
                        .key(&format!("Key {}", i))
                        .headers(OwnedHeaders::new().add("header_key", "header_value")),
                    Duration::from_secs(0),
                )
                .await;

            // This will be executed when the result is received.
            info!("Delivery status for message {} received", i);
            delivery_status
        })
        .collect::<Vec<_>>();

    // This loop will wait until all delivery statuses have been received.
    for future in futures {
        info!("Future completed. Result: {:?}", future.await);
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_event_queue(
        &Pubkey::from_str("9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin")?,
        &Pubkey::from_str("9wFFyRfZBsuAha4YcuxcXLKwMxJR43S7fPfQLusDBzvT")?,
    )
    // let matches = App::new("producer example")
    //     .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
    //     .about("Simple command line producer")
    //     .arg(
    //         Arg::new("brokers")
    //             .short('b')
    //             .long("brokers")
    //             .help("Broker list in kafka format")
    //             .takes_value(true)
    //             .default_value("localhost:9092"),
    //     )
    //     .arg(
    //         Arg::new("log-conf")
    //             .long("log-conf")
    //             .help("Configure the logging format (example: 'rdkafka=trace')")
    //             .takes_value(true),
    //     )
    //     .arg(
    //         Arg::new("topic")
    //             .short('t')
    //             .long("topic")
    //             .help("Destination topic")
    //             .takes_value(true)
    //             .default_value("hello-world"),
    //     )
    //     .get_matches();
    //
    // setup_logger(true, matches.value_of("log-conf"));
    //
    // let (version_n, version_s) = get_rdkafka_version();
    // info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);
    //
    // let topic = matches.value_of("topic").unwrap();
    // let brokers = matches.value_of("brokers").unwrap();
    //
    // produce(brokers, topic).await;
}
