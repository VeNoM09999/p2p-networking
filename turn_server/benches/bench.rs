#![allow(dead_code, unused)]
use core::panic::PanicInfo;
use std::{
    fs::File,
    io::{BufRead, BufReader, Read},
    time::Duration,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open("./../runtime.log")?;
    let mut bufreader = BufReader::new(file);
    let mut line = String::new();
    let mut latencies = Vec::<u64>::new();
    loop {
        line.clear();
        let bytes = bufreader.read_line(&mut line)?;

        if bytes == 0 {
            break;
        }

        let latency = parse_log(&line);
        if let Some(t) = latency {
            latencies.push(t);
        }
    }
    if latencies.len() > 0 {
        latencies.sort_unstable();
        let percentile_p90 = calculate_percentile(latencies.len(), 0.90, &latencies);
        let percentile_p95 = calculate_percentile(latencies.len(), 0.95, &latencies);
        let percentile_p99 = calculate_percentile(latencies.len(), 0.99, &latencies);
        println!(
            "Log size: {:#?}\r\nP90: {:#?}\r\nP95: {:#?}\r\nP99: {:#?}\r\n",
            latencies.len(),
            Duration::from_nanos(percentile_p90),
            Duration::from_nanos(percentile_p95),
            Duration::from_nanos(percentile_p99),
        );
    } else {
        println!("failure to calculate percentile");
    }
    Ok(())
}

fn parse_log(string: &str) -> Option<u64> {
    for part in string.split_ascii_whitespace() {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };

        if key == "total" {
            return parse_duration_ns(value);
        }
    }

    None
}

fn parse_duration_ns(value: &str) -> Option<u64> {
    if let Some(nano) = value.strip_suffix("ns") {
        nano.parse().ok()
    } else if let Some(micro) = value.strip_suffix("µs") {
        let n: f64 = micro.parse().ok()?;
        Some((n * 1_000.0) as u64)
    } else if let Some(mili) = value.strip_suffix("ms") {
        let n: f64 = mili.parse().ok()?;
        Some((n * 1_000_000.0) as u64)
    } else {
        None
    }
}

fn calculate_percentile(total: usize, percentile: f32, latencies: &Vec<u64>) -> u64 {
    let index = ((total as f32 * percentile).ceil() as usize).saturating_sub(1);

    latencies[index]
}
