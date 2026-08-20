FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/payledger-server ./cmd/server \
    && CGO_ENABLED=0 go build -trimpath -o /out/payledger-client ./cmd/client \
    && CGO_ENABLED=0 go build -trimpath -o /out/payledger-load ./cmd/load

FROM alpine:3.22
RUN addgroup -S payledger && adduser -S -G payledger payledger
COPY --from=build /out/payledger-* /usr/local/bin/
USER payledger
ENTRYPOINT ["payledger-server"]
