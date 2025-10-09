/// Test data generation utilities for comprehensive benchmarking
use datafusion::arrow::array::StringArray;
use std::fmt::Write as FmtWrite;

/// Configuration for generating test datasets
#[derive(Debug, Clone)]
pub struct DataConfig {
    pub size: usize,
    pub string_length: StringLength,
    pub match_rate: f64, // 0.0 to 1.0
    pub null_rate: f64,  // 0.0 to 1.0
}

#[derive(Debug, Clone, Copy)]
pub enum StringLength {
    Short,  // 10-20 chars
    Medium, // 50-100 chars
    Long,   // 500-1000 chars
}

impl StringLength {
    fn range(&self) -> (usize, usize) {
        match self {
            StringLength::Short => (10, 20),
            StringLength::Medium => (50, 100),
            StringLength::Long => (500, 1000),
        }
    }
}

/// Pattern complexity levels
#[derive(Debug, Clone, Copy)]
pub enum PatternComplexity {
    Simple,  // Literal or basic pattern
    Medium,  // Multiple capturing groups
    Complex, // Complex alternations and groups
}

/// Generates test data for simple email-like pattern: word-digits
pub fn generate_email_like_data(config: &DataConfig) -> StringArray {
    let mut data = Vec::with_capacity(config.size);
    let (min_len, max_len) = config.string_length.range();

    for i in 0..config.size {
        // Determine if this entry should be null
        if (i as f64 / config.size as f64) < config.null_rate {
            data.push(None);
            continue;
        }

        // Determine if this should match
        let should_match = (i as f64 / config.size as f64) >= config.null_rate
            && ((i - (config.size as f64 * config.null_rate) as usize) as f64
                / (config.size as f64 * (1.0 - config.null_rate)))
                < config.match_rate;

        if should_match {
            // Generate matching string: prefix-123
            let prefix = format!("user{}", i % 1000);
            let number = 100 + (i % 900);
            let mut s = format!("{}-{}", prefix, number);

            // Pad to desired length
            while s.len() < min_len {
                s.push_str(&format!("_{}", i % 10));
            }
            if s.len() > max_len {
                s.truncate(max_len);
            }

            data.push(Some(s));
        } else {
            // Generate non-matching string
            let mut s = format!("nomatch{}", i);
            while s.len() < min_len {
                s.push_str("_xyz");
            }
            if s.len() > max_len {
                s.truncate(max_len);
            }
            data.push(Some(s));
        }
    }

    StringArray::from(data)
}

/// Generates test data for log line pattern matching
pub fn generate_log_lines(config: &DataConfig) -> StringArray {
    let mut data = Vec::with_capacity(config.size);
    let (min_len, max_len) = config.string_length.range();

    let log_levels = ["INFO", "WARN", "ERROR", "DEBUG"];
    let components = ["Server", "Database", "Cache", "API", "Worker"];

    for i in 0..config.size {
        if (i as f64 / config.size as f64) < config.null_rate {
            data.push(None);
            continue;
        }

        let should_match = (i as f64 / config.size as f64) >= config.null_rate
            && ((i - (config.size as f64 * config.null_rate) as usize) as f64
                / (config.size as f64 * (1.0 - config.null_rate)))
                < config.match_rate;

        if should_match {
            // Generate matching log line: [LEVEL] Component: message
            let level = log_levels[i % log_levels.len()];
            let component = components[i % components.len()];
            let mut s = format!(
                "[{}] {}: Operation completed successfully",
                level, component
            );

            // Add padding
            while s.len() < min_len {
                s.push_str(" | extra context data");
            }
            if s.len() > max_len {
                s.truncate(max_len);
            }

            data.push(Some(s));
        } else {
            // Non-matching format
            let mut s = format!("Invalid log entry at index {}", i);
            while s.len() < min_len {
                s.push_str(" with more data");
            }
            if s.len() > max_len {
                s.truncate(max_len);
            }
            data.push(Some(s));
        }
    }

    StringArray::from(data)
}

/// Generates test data for URL pattern matching
pub fn generate_urls(config: &DataConfig) -> StringArray {
    let mut data = Vec::with_capacity(config.size);
    let (min_len, max_len) = config.string_length.range();

    let protocols = ["http", "https"];
    let domains = ["example.com", "test.org", "demo.net", "api.io"];

    for i in 0..config.size {
        if (i as f64 / config.size as f64) < config.null_rate {
            data.push(None);
            continue;
        }

        let should_match = (i as f64 / config.size as f64) >= config.null_rate
            && ((i - (config.size as f64 * config.null_rate) as usize) as f64
                / (config.size as f64 * (1.0 - config.null_rate)))
                < config.match_rate;

        if should_match {
            let protocol = protocols[i % protocols.len()];
            let domain = domains[i % domains.len()];
            let mut s = format!("{}://{}/path/to/resource", protocol, domain);

            // Add query params to reach desired length
            while s.len() < min_len {
                s.push_str(&format!("&param{}=value{}", i % 10, i % 100));
            }
            if s.len() > max_len {
                s.truncate(max_len);
            }

            data.push(Some(s));
        } else {
            let mut s = format!("not-a-url-{}", i);
            while s.len() < min_len {
                s.push_str("-more-text");
            }
            if s.len() > max_len {
                s.truncate(max_len);
            }
            data.push(Some(s));
        }
    }

    StringArray::from(data)
}

/// Generates test data for complex pattern with multiple groups
pub fn generate_structured_data(config: &DataConfig) -> StringArray {
    let mut data = Vec::with_capacity(config.size);
    let (min_len, max_len) = config.string_length.range();

    for i in 0..config.size {
        if (i as f64 / config.size as f64) < config.null_rate {
            data.push(None);
            continue;
        }

        let should_match = (i as f64 / config.size as f64) >= config.null_rate
            && ((i - (config.size as f64 * config.null_rate) as usize) as f64
                / (config.size as f64 * (1.0 - config.null_rate)))
                < config.match_rate;

        if should_match {
            // Format: KEY=value|ID:12345|STATUS=active
            let mut s = String::new();
            write!(
                s,
                "KEY=item{}|ID:{}|STATUS={}",
                i % 100,
                10000 + (i % 90000),
                if i % 2 == 0 { "active" } else { "pending" }
            )
            .unwrap();

            // Pad to desired length
            while s.len() < min_len {
                write!(s, "|EXTRA=data{}", i % 1000).unwrap();
            }
            if s.len() > max_len {
                s.truncate(max_len);
            }

            data.push(Some(s));
        } else {
            let mut s = format!("unstructured_data_{}", i);
            while s.len() < min_len {
                s.push_str("_random");
            }
            if s.len() > max_len {
                s.truncate(max_len);
            }
            data.push(Some(s));
        }
    }

    StringArray::from(data)
}

/// Generates test data with all nulls (stress test for null handling)
pub fn generate_all_nulls(size: usize) -> StringArray {
    let data: Vec<Option<&str>> = vec![None; size];
    StringArray::from(data)
}

/// Generates test data with identical strings (best case for caching)
pub fn generate_identical_strings(size: usize, pattern_matches: bool) -> StringArray {
    let s = if pattern_matches {
        "abc-123"
    } else {
        "nomatch"
    };
    StringArray::from(vec![Some(s); size])
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;

    #[test]
    fn test_email_like_generation() {
        let config = DataConfig {
            size: 100,
            string_length: StringLength::Short,
            match_rate: 0.8,
            null_rate: 0.1,
        };
        let arr = generate_email_like_data(&config);
        assert_eq!(arr.len(), 100);
    }

    #[test]
    fn test_all_nulls() {
        let arr = generate_all_nulls(50);
        assert_eq!(arr.len(), 50);
        assert_eq!(arr.null_count(), 50);
    }

    #[test]
    fn test_identical_strings() {
        let arr = generate_identical_strings(100, true);
        assert_eq!(arr.len(), 100);
    }
}
