/// CSV data generator for benchmarks
///
/// Run with: cargo run --bin generate_csv_data
///
/// Generates static CSV files in data/ directory for reproducible benchmarks.
use std::fs::{self, File};
use std::io::{BufWriter, Write};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Generating CSV benchmark data...\n");

    // Create data directories
    fs::create_dir_all("data/scalar_pattern")?;
    fs::create_dir_all("data/match_rates")?;
    fs::create_dir_all("data/null_rates")?;
    fs::create_dir_all("data/string_lengths")?;
    fs::create_dir_all("data/realistic")?;
    fs::create_dir_all("data/column_patterns")?;
    fs::create_dir_all("data/edge_cases")?;

    // Scalar pattern: different sizes
    generate_scalar_pattern_files()?;

    // Match rates
    generate_match_rate_files()?;

    // Null rates
    generate_null_rate_files()?;

    // String lengths
    generate_string_length_files()?;

    // Realistic workloads
    generate_realistic_files()?;

    // Column patterns
    generate_column_pattern_files()?;

    // Edge cases
    generate_edge_case_files()?;

    println!("\n✅ All CSV files generated successfully!");
    println!("\nGenerated files:");
    println!("  data/scalar_pattern/*.csv    - 1K, 10K, 100K rows");
    println!("  data/match_rates/*.csv       - 0%, 25%, 50%, 75%, 100% match rates");
    println!("  data/null_rates/*.csv        - 0%, 20%, 50% null rates");
    println!("  data/string_lengths/*.csv    - short, medium, long strings");
    println!("  data/realistic/*.csv         - logs, urls, structured data");
    println!("  data/column_patterns/*.csv   - low, mid, high cardinality");
    println!("  data/edge_cases/*.csv        - all_nulls, identical strings");

    Ok(())
}

fn generate_scalar_pattern_files() -> Result<(), Box<dyn std::error::Error>> {
    println!("Generating scalar_pattern files...");

    for &size in &[1_000, 10_000, 100_000] {
        let path = format!("data/scalar_pattern/{}k.csv", size / 1000);
        let mut writer = BufWriter::new(File::create(&path)?);

        writeln!(writer, "text,expected_group1,expected_group2")?;

        for i in 0..size {
            let text = if i % 5 != 0 {
                format!("user{}-{}", i % 1000, 100 + (i % 900))
            } else {
                format!("nomatch{}", i)
            };

            let (g1, g2) = if i % 5 != 0 {
                (format!("user{}", i % 1000), format!("{}", 100 + (i % 900)))
            } else {
                ("".to_string(), "".to_string())
            };

            writeln!(writer, "{},{},{}", text, g1, g2)?;
        }

        println!("  ✓ {}", path);
    }

    Ok(())
}

fn generate_match_rate_files() -> Result<(), Box<dyn std::error::Error>> {
    println!("Generating match_rates files...");
    let size = 10_000;

    for &rate in &[0.0, 0.25, 0.5, 0.75, 1.0] {
        let filename = format!("data/match_rates/{}pct.csv", (rate * 100.0) as i32);
        let mut writer = BufWriter::new(File::create(&filename)?);

        writeln!(writer, "text")?;

        for i in 0..size {
            let should_match = (i as f64 / size as f64) < rate;
            let text = if should_match {
                format!("user{}-{}", i % 1000, 100 + (i % 900))
            } else {
                format!("nomatch{}", i)
            };
            writeln!(writer, "{}", text)?;
        }

        println!("  ✓ {}", filename);
    }

    Ok(())
}

fn generate_null_rate_files() -> Result<(), Box<dyn std::error::Error>> {
    println!("Generating null_rates files...");
    let size = 10_000;

    for &rate in &[0.0, 0.2, 0.5] {
        let filename = format!("data/null_rates/{}pct.csv", (rate * 100.0) as i32);
        let mut writer = BufWriter::new(File::create(&filename)?);

        writeln!(writer, "text")?;

        for i in 0..size {
            let is_null = (i as f64 / size as f64) < rate;
            if is_null {
                writeln!(writer)?; // Empty for NULL
            } else {
                writeln!(writer, "user{}-{}", i % 1000, 100 + (i % 900))?;
            }
        }

        println!("  ✓ {}", filename);
    }

    Ok(())
}

fn generate_string_length_files() -> Result<(), Box<dyn std::error::Error>> {
    println!("Generating string_lengths files...");
    let size = 10_000;

    let lengths = [("short", 10, 20), ("medium", 50, 100), ("long", 500, 1000)];

    for (name, min_len, max_len) in lengths.iter() {
        let filename = format!("data/string_lengths/{}.csv", name);
        let mut writer = BufWriter::new(File::create(&filename)?);

        writeln!(writer, "text")?;

        for i in 0..size {
            let mut text = format!("user{}-{}", i % 1000, 100 + (i % 900));

            // Pad to target length
            while text.len() < *min_len {
                text.push_str(&format!("_pad{}", i % 10));
            }

            if text.len() > *max_len {
                text.truncate(*max_len);
            }

            writeln!(writer, "{}", text)?;
        }

        println!("  ✓ {}", filename);
    }

    Ok(())
}

fn generate_realistic_files() -> Result<(), Box<dyn std::error::Error>> {
    println!("Generating realistic workload files...");
    let size = 10_000;

    // Log lines
    {
        let mut writer = BufWriter::new(File::create("data/realistic/logs.csv")?);
        writeln!(writer, "log_line,expected_level,expected_component")?;

        let levels = ["INFO", "WARN", "ERROR", "DEBUG"];
        let components = ["Server", "Database", "Cache", "API", "Worker"];

        for i in 0..size {
            let level = levels[i % levels.len()];
            let component = components[i % components.len()];
            let log = format!("[{}] {}: Operation completed", level, component);
            writeln!(writer, "{},{},{}", log, level, component)?;
        }

        println!("  ✓ data/realistic/logs.csv");
    }

    // URLs
    {
        let mut writer = BufWriter::new(File::create("data/realistic/urls.csv")?);
        writeln!(writer, "url,expected_protocol,expected_domain")?;

        let protocols = ["http", "https"];
        let domains = ["example.com", "test.org", "demo.net", "api.io"];

        for i in 0..size {
            let protocol = protocols[i % protocols.len()];
            let domain = domains[i % domains.len()];
            let url = format!("{}://{}/path/resource{}", protocol, domain, i);
            writeln!(writer, "{},{},{}", url, protocol, domain)?;
        }

        println!("  ✓ data/realistic/urls.csv");
    }

    // Structured data
    {
        let mut writer = BufWriter::new(File::create("data/realistic/structured.csv")?);
        writeln!(writer, "data,expected_id")?;

        for i in 0..size {
            let id = 10000 + (i % 90000);
            let data = format!("KEY=item{}|ID:{}|STATUS=active", i % 100, id);
            writeln!(writer, "{},{}", data, id)?;
        }

        println!("  ✓ data/realistic/structured.csv");
    }

    Ok(())
}

fn generate_column_pattern_files() -> Result<(), Box<dyn std::error::Error>> {
    println!("Generating column_patterns files...");
    let size = 10_000;

    let base_patterns = [
        r"(\w+)-(\d+)",
        r"([a-z]+)_(\d{3})",
        r"^user(\d+)$",
        r"(\w{3,5})-(\d{2,4})",
    ];

    // Low cardinality (4 unique patterns)
    {
        let mut writer = BufWriter::new(File::create("data/column_patterns/low_cardinality.csv")?);
        writeln!(writer, "text,pattern")?;

        for i in 0..size {
            let text = format!("user{}-{}", i % 1000, 100 + (i % 900));
            let pattern = &base_patterns[i % base_patterns.len()];
            writeln!(writer, "{},{}", text, pattern)?;
        }

        println!("  ✓ data/column_patterns/low_cardinality.csv");
    }

    // Mid cardinality (128 unique patterns)
    {
        let mut writer = BufWriter::new(File::create("data/column_patterns/mid_cardinality.csv")?);
        writeln!(writer, "text,pattern")?;

        for i in 0..size {
            let text = format!("user{}-{}", i % 1000, 100 + (i % 900));
            let variant = i % 128;
            let base = &base_patterns[variant % base_patterns.len()];
            let pattern = format!("{}|extra{}", base, variant);
            writeln!(writer, "{},{}", text, pattern)?;
        }

        println!("  ✓ data/column_patterns/mid_cardinality.csv");
    }

    // High cardinality (all unique)
    {
        let mut writer = BufWriter::new(File::create("data/column_patterns/high_cardinality.csv")?);
        writeln!(writer, "text,pattern")?;

        for i in 0..size {
            let text = format!("user{}-{}", i % 1000, 100 + (i % 900));
            let pattern = format!(r"(\w+)-(\d{{{}}})", (i % 5) + 1);
            writeln!(writer, "{},{}", text, pattern)?;
        }

        println!("  ✓ data/column_patterns/high_cardinality.csv");
    }

    Ok(())
}

fn generate_edge_case_files() -> Result<(), Box<dyn std::error::Error>> {
    println!("Generating edge_cases files...");
    let size = 10_000;

    // All nulls
    {
        let mut writer = BufWriter::new(File::create("data/edge_cases/all_nulls.csv")?);
        writeln!(writer, "text")?;

        for _ in 0..size {
            writeln!(writer)?; // Empty for NULL
        }

        println!("  ✓ data/edge_cases/all_nulls.csv");
    }

    // Identical matching
    {
        let mut writer = BufWriter::new(File::create("data/edge_cases/identical_matching.csv")?);
        writeln!(writer, "text")?;

        for _ in 0..size {
            writeln!(writer, "abc-123")?;
        }

        println!("  ✓ data/edge_cases/identical_matching.csv");
    }

    // Identical non-matching
    {
        let mut writer =
            BufWriter::new(File::create("data/edge_cases/identical_non_matching.csv")?);
        writeln!(writer, "text")?;

        for _ in 0..size {
            writeln!(writer, "nomatch")?;
        }

        println!("  ✓ data/edge_cases/identical_non_matching.csv");
    }

    Ok(())
}
