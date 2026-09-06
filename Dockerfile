FROM golang:1.25.13 AS build

ARG FORNIX_VERSION=dev
ARG FORNIX_COMMIT=unknown
ARG FORNIX_BUILD_DATE=unknown

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X github.com/omaveda/fornix/internal/version.Version=${FORNIX_VERSION} -X github.com/omaveda/fornix/internal/version.Commit=${FORNIX_COMMIT} -X github.com/omaveda/fornix/internal/version.Date=${FORNIX_BUILD_DATE}" -o /out/fornix ./cmd/fornix

FROM debian:bookworm-slim

RUN useradd --system --uid 10001 --create-home fornix
COPY --from=build /out/fornix /usr/local/bin/fornix
USER fornix
EXPOSE 8201
ENTRYPOINT ["/usr/local/bin/fornix"]
