#############
### BUILD ###
#############
FROM golang:1.13-alpine as build
RUN apk update && apk upgrade && apk add --no-cache gcc musl-dev postgresql
RUN mkdir -p /tmp/gonymizer/bin
WORKDIR /tmp/gonymizer/
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -v -ldflags '-w -extldflags "-static"' -o bin/gonymizer ./cmd/...

##########################
### Gonymizer Runtime  ###
##########################
FROM golang:1.13-alpine as gonymizer
RUN apk update && apk upgrade && apk add --no-cache postgresql

COPY --from=build /tmp/gonymizer/bin/gonymizer /usr/bin/gonymizer
