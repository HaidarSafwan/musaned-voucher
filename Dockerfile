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

# OpenShift runs containers with a random UID assigned to group 0 (root).
# chown 1001:0 + chmod g=u ensures the random UID can read/write all files.
RUN mkdir -p data/uploads data/results && \
    chown -R 1001:0 /app && \
    chmod -R g=u /app

# Mount /app/data as a volume to persist uploads, results, and jobs.json
VOLUME ["/app/data"]

EXPOSE 8081

USER 1001

# Mount config.json at /app/config.json or pass path as first argument
ENTRYPOINT ["./musaned-voucher"]
