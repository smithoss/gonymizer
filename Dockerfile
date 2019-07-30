#############
### BUILD ###
#############
FROM golang:alpine as build
ENV GO111MODULE=on
RUN apk update && apk upgrade && apk add --no-cache bash git gcc go linux-headers musl-dev postgresql curl
RUN mkdir -p /usr/local/go/src/github.com/smithoss/gonymizer/
WORKDIR /usr/local/go/src/github.com/smithoss/gonymizer/
COPY . /usr/local/go/src/github.com/smithoss/gonymizer/
RUN go mod download
WORKDIR /usr/local/go/src/github.com/smithoss/gonymizer/cmd/
RUN GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -v -ldflags '-w -extldflags "-static"' -o ../gonymizer

##########################
### Gonymizer Runtime  ###
##########################
FROM alpine as gonymizer
RUN apk update && apk upgrade && apk add --no-cache postgresql curl

COPY --from=build /usr/local/go/src/github.com/smithoss/gonymizer/gonymizer /usr/bin/gonymizer
