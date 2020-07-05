FROM golang:1.14-alpine AS base

RUN apk --no-cache upgrade && apk --no-cache add git make
COPY . /app/
WORKDIR /app/
RUN go mod download

FROM base AS projectbuilder
RUN make build-masenko

FROM alpine
RUN apk --no-cache upgrade && apk --no-cache add ca-certificates
COPY --from=projectbuilder /app/bin/ /usr/local/bin/
WORKDIR /home/
CMD ["masenko"]

