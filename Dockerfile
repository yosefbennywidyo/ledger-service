FROM golang:1.26-bookworm AS build
WORKDIR /src
ENV GOTOOLCHAIN=auto
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/ledger-service ./cmd/ledger-service

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget
COPY --from=build /out/ledger-service /ledger-service
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=10 \
  CMD wget -q -O- http://localhost:8080/healthz || exit 1
ENTRYPOINT ["/ledger-service"]
