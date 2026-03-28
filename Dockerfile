
# Builder stage
FROM golang:1.25.5-alpine AS builder

# Add git for some go modules
RUN apk add --no-cache git

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=v0.1.0
ARG COMMIT=dev
ARG BUILD_DATE=unknown

RUN CGO_ENABLED=0 GOOS=linux go build -o /mog -ldflags "-X mog/internal/version.Version=${VERSION} -X mog/internal/version.Commit=${COMMIT} -X mog/internal/version.BuildDate=${BUILD_DATE}" cmd/mog/main.go

# Production stage
FROM alpine:3.20

LABEL org.opencontainers.image.source="https://github.com/monkdbofficial/mog"
LABEL org.opencontainers.image.description="MongoDB wire protocol proxy for MonkDB"
LABEL org.opencontainers.image.licenses="MIT"

# Add ca-certificates for secure connections
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /root/

COPY --from=builder /mog .

# MongoDB wire protocol port
EXPOSE 27017 
# Metrics exporter port
EXPOSE 8080

CMD ["./mog"]
