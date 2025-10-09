# StringView Implementation - Phase 1 Issues & Fixes

## Current Errors

1. **StringViewBuilder::with_capacity()**takes only 1 argument (not 2)
2. **StringViewArray doesn't have .values()** method (different from StringArray)
3. **ScalarValue::Utf8View** scalar handling needs fix

## Fixes Needed

### Fix 1: Builder Creation (Line 263)
```rust
// Wrong:
let mut builder = StringViewBuilder::with_capacity(n, data_capacity);

// Right:
let mut builder = StringViewBuilder::with_capacity(n);
// Note: StringViewBuilder doesn't take data_capacity parameter
```

### Fix 2: byte_capacity_hint for StringViewArray (Line ~560)
```rust
// Wrong:
} else if let Some(arr) = self.utf8_view {
    arr.values().len()  // ❌ StringViewArray has no .values()
}

// Right:
} else if let Some(arr) = self.utf8_view {
    // StringView stores data in buffers, estimate from data_buffers
    arr.data_buffers().iter().map(|buf| buf.len()).sum()
}
```

### Fix 3: Utf8View Scalar Handling (Line ~452)
```rust
// Wrong:
ScalarValue::Utf8View(opt) => Ok(Self {
    scalar: Some(opt.map(|v| v.to_string())),  // ❌ opt is &str, can't move
    ...
})

// Right:
ScalarValue::Utf8View(opt) => Ok(Self {
    scalar: Some(opt.clone()),  // Clone the Option<String>
    ...
})
```

## Current

 Status

These are simple fixes once we understand the APIs. Let me apply them now systematically.
