use datafusion::arrow::array::{Array, StringArray};
use df_regexp_extract::eval_regexp_extract_utf8;
use regex::Regex;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== DataFrame Regexp Extract Demo ===\n");
    
    // Example 1: Basic pattern matching
    let data = StringArray::from(vec![
        Some("abc-123"),
        Some("xyz-456"),
        Some("no-match"),
        None,
    ]);
    
    let pattern = Regex::new(r"(\w+)-(\d+)")?;
    
    // Extract group 1 (letters)
    println!("Extracting group 1 (letters):");
    let result1 = eval_regexp_extract_utf8(&data, &pattern, 1)?;
    let str_result1 = result1.as_any().downcast_ref::<StringArray>().unwrap();
    for i in 0..str_result1.len() {
        if str_result1.is_null(i) {
            println!("  [{}] NULL", i);
        } else {
            println!("  [{}] '{}'", i, str_result1.value(i));
        }
    }
    
    // Extract group 2 (digits)
    println!("\nExtracting group 2 (digits):");
    let result2 = eval_regexp_extract_utf8(&data, &pattern, 2)?;
    let str_result2 = result2.as_any().downcast_ref::<StringArray>().unwrap();
    for i in 0..str_result2.len() {
        if str_result2.is_null(i) {
            println!("  [{}] NULL", i);
        } else {
            println!("  [{}] '{}'", i, str_result2.value(i));
        }
    }
    
    // Extract entire match (group 0)
    println!("\nExtracting entire match (group 0):");
    let result0 = eval_regexp_extract_utf8(&data, &pattern, 0)?;
    let str_result0 = result0.as_any().downcast_ref::<StringArray>().unwrap();
    for i in 0..str_result0.len() {
        if str_result0.is_null(i) {
            println!("  [{}] NULL", i);
        } else {
            println!("  [{}] '{}'", i, str_result0.value(i));
        }
    }
    
    println!("\n=== Demo completed successfully! ===");
    Ok(())
}
