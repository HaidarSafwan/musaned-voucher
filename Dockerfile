# Stage 1: build
FROM golang:1.22-alpine AS builder

WORKDIR /build

# Download dependencies before copying source (layer cache)
COPY go.mod go.sum ./
RUN go mod download

# Compile a fully static binary — go-ora is pure Go so CGO is not needed
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o musaned-voucher .

# Stage 2: runtime — minimal image, no Go toolchain
FROM alpine:3.20

WORKDIR /app

COPY --from=builder /build/musaned-voucher .

# Directories the service writes to; override with volume mounts in production
RUN mkdir -p uploads results

EXPOSE 8081

# Pass config path as first arg, or mount config.json at /app/config.json
ENTRYPOINT ["./musaned-voucher"]
