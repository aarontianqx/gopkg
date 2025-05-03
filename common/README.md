# Common Package

Provides essential utilities for Go applications, focusing on logging, runtime management, and unique ID generation.

## Key Features

- **Structured Logging:** Context-aware logger (`LoggerCtx`) that automatically includes request/trace IDs (`LogID`) when available. Uses `slog` for structured output.
- **Runtime Utilities:** Functions for panic recovery (`Recover`) and retrieving runtime information like function names (`GetFuncName`) and Goroutine IDs (`GetGoroutineID`).
- **Log ID Management:** Utilities (`LogID`, `WithLogID`, `NewLogID`) for generating and propagating unique identifiers through context, useful for tracing requests.
- **Network Utilities:** Basic network helper functions (e.g., `GetLocalIP`). 

## Usage Examples

### Logger Configuration

Configure the logger at application startup:

```go
import (
    "log/slog"
    "os"
    
    "github.com/aarontianqx/gopkg/common"
    "github.com/aarontianqx/gopkg/common/logimpl"
)

func main() {
    // Initialize logger with custom configuration
    common.InitLogger(
        logimpl.WithLevel(slog.LevelDebug), // LogLevel
        logimpl.WithAddSource(true),        // Include source file and line numbers
        logimpl.WithOutput(os.Stdout),      // Output destination
        logimpl.WithFormat("json"),         // "json" or "text"
    )
    
    // Use the logger anywhere in your code
    common.Logger().Info("Server starting", "port", 8080)
    
    // With context (automatically includes trace IDs, request IDs, etc.)
    common.LoggerCtx(ctx).Debug("Processing request", "method", "GET")
} 
```
