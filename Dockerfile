FROM golang:1.12-alpine
MAINTAINER Leif Johansson <leifj@sunet.se>
RUN apk add --update --no-cache git
WORKDIR /go/src/thiss-go-mdq
COPY . .
RUN go get -d -v ./...
RUN go install -v ./...
ENV METADATA "/etc/metadata.json"
EXPOSE 3000
ENTRYPOINT ["thiss-go-mdq"]
