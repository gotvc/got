# Builder Container
FROM golang:1.20.5-alpine3.18 as builder
RUN mkdir /app
RUN mkdir /out
WORKDIR /app

## Dependencies
COPY go.sum .
COPY go.mod .
RUN go mod download -x

## Compile
COPY . .
RUN CGO_ENABLED=0 go build -v -o /out/got ./cmd/got

# Final Container
FROM alpine:3.18
COPY --from=builder /out/got /usr/bin/got
# QUIC
EXPOSE 6666

# Mount the repo in at /repo
WORKDIR /repo
CMD got serve-quic 0.0.0.0:6666
