# Error Handling: Zero Policy

## The Sacred Law
All fallible operations return `Result<T, Error>`. Capturing error information is a requirement, not a suggestion.

## combinators
Use `map`, `and_then`, and `?` to propagate errors idiomatically.
