# `getParameter` / `featureCompatibilityVersion`

## Usage

```javascript
db.runCommand({ getParameter: 1, featureCompatibilityVersion: 1 })
db.runCommand({ featureCompatibilityVersion: 1 })
```

## Behavior

Returns a fixed FCV document consistent with MoG’s reported capabilities.

## Code pointers

- `internal/mongo/handler.go`
