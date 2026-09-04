FROM golang:1.27.1 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/fornix ./cmd/fornix

FROM debian:bookworm-slim

RUN useradd --system --uid 10001 --create-home fornix
COPY --from=build /out/fornix /usr/local/bin/fornix
USER fornix
EXPOSE 8201
ENTRYPOINT ["/usr/local/bin/fornix"]
