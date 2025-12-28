use std::sync::Once;

use time::{format_description::BorrowedFormatItem, macros::format_description};
use tracing_appender::{non_blocking::{NonBlocking, WorkerGuard}, rolling};
use tracing_subscriber::fmt::{self, time::OffsetTime};


/// Global logger initialization (once per test process)
static INIT_LOGGER: Once = Once::new();

/// Initialize the logger for tests - writes to logs directory
#[ctor::ctor]
pub fn init_test_logger() {
    INIT_LOGGER.call_once(|| {
		// Get the test binary name to create separate log files per test binary
		let binary_name = std::env::current_exe()
			.ok()
			.and_then(|p| p.file_stem().map(|s| s.to_string_lossy().into_owned()))
			.unwrap_or_else(|| "unknown".to_string());
		// Remove hash suffix (e.g., "agree_tests-a1b2c3d4" -> "agree_tests")
		let clean_name = binary_name
			.rfind('-')
			.map(|pos| &binary_name[..pos])
			.unwrap_or(&binary_name);
		let log_filename = format!("latest-{}.log", clean_name);
		
		let file_writer = get_truncate_writer(&log_filename);
        tracing_subscriber::fmt()
			.with_timer(get_local_timer())
			.with_level(true)
            .with_writer(file_writer)
			.with_target(true)
			.with_span_events(fmt::format::FmtSpan::NONE)
            .with_ansi(false)
            .init();
    });
}

fn get_truncate_writer(filename: &str) -> std::fs::File {
	use std::fs::OpenOptions;

	let mut file = OpenOptions::new()
		.write(true)
		.create(true)
		.truncate(true)
		.open(format!("logs/{}", filename))
		.expect("Failed to open log file for truncation");
	file
}

fn get_rolling_writer(filename: &str) -> (NonBlocking, WorkerGuard) {
	let file_appender = rolling::daily("logs", filename);
	let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
	(non_blocking, guard)
}

fn get_local_timer() -> OffsetTime<&'static [BorrowedFormatItem<'static>]> {
    // The `format_description!` macro requires the "macros" feature in the time crate.
    let time_format = format_description!("[month]-[day] [hour]:[minute]:[second].[subsecond digits:3]");

    // Retrieve the current local offset from the system
    let time_offset = time::UtcOffset::current_local_offset()
        .expect("Could not determine local time offset");

    // Create the timer with the local offset and format
    let timer = OffsetTime::new(time_offset, time_format);
    timer
}

#[macro_export]
macro_rules! tassert {
	($cond:expr $(,)?) => {{
		if !$cond {
			::tracing::error!(condition = %stringify!($cond), "ASSERT FAILED");
		}
		assert!($cond);
	}};
	($cond:expr, $($arg:tt)+) => {{
		let user_message = format!("{}", format_args!($($arg)+));
		if !$cond {
			::tracing::error!(
				condition = %stringify!($cond),
				message = %user_message,
				"ASSERT FAILED"
			);
		}
		assert!($cond, $($arg)+);
	}};
}

#[macro_export]
macro_rules! tassert_eq {
	($left:expr, $right:expr $(,)?) => {{
		let left_val = &$left;
		let right_val = &$right;
		if left_val != right_val {
			::tracing::error!(
				left = ?left_val,
				right = ?right_val,
				left_expr = %stringify!($left),
				right_expr = %stringify!($right),
				"ASSERT_EQ FAILED"
			);
		}
		assert_eq!(left_val, right_val);
	}};
	($left:expr, $right:expr, $($arg:tt)+) => {{
		let left_val = &$left;
		let right_val = &$right;
		let user_message = format!("{}", format_args!($($arg)+));
		if left_val != right_val {
			::tracing::error!(
				left = ?left_val,
				right = ?right_val,
				left_expr = %stringify!($left),
				right_expr = %stringify!($right),
				message = %user_message,
				"ASSERT_EQ FAILED"
			);
		}
		assert_eq!(left_val, right_val, $($arg)+);
	}};
}

#[macro_export]
macro_rules! tassert_ne {
	($left:expr, $right:expr $(,)?) => {{
		let left_val = &$left;
		let right_val = &$right;
		if left_val == right_val {
			::tracing::error!(
				left = ?left_val,
				right = ?right_val,
				left_expr = %stringify!($left),
				right_expr = %stringify!($right),
				"ASSERT_NE FAILED"
			);
		}
		assert_ne!(left_val, right_val);
	}};
	($left:expr, $right:expr, $($arg:tt)+) => {{
		let left_val = &$left;
		let right_val = &$right;
		let user_message = format!("{}", format_args!($($arg)+));
		if left_val == right_val {
			::tracing::error!(
				left = ?left_val,
				right = ?right_val,
				left_expr = %stringify!($left),
				right_expr = %stringify!($right),
				message = %user_message,
				"ASSERT_NE FAILED"
			);
		}
		assert_ne!(left_val, right_val, $($arg)+);
	}};
}

#[macro_export]
macro_rules! tpanic {
	() => {{
		::tracing::error!("PANIC");
		panic!();
	}};
	($msg:literal $(,)?) => {{
		let user_message = format!("{}", format_args!($msg));
		::tracing::error!(message = %user_message, "PANIC");
		panic!($msg);
	}};
	($fmt:literal, $($arg:tt)+) => {{
		let user_message = format!("{}", format_args!($fmt, $($arg)+));
		::tracing::error!(message = %user_message, "PANIC");
		panic!($fmt, $($arg)+);
	}};
	($payload:expr $(,)?) => {{
		let payload_ref = &$payload;
		::tracing::error!(payload = ?payload_ref, "PANIC");
		panic!($payload);
	}};
}
