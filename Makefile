VERSION:=1.0.0
NAME:=thiss-go-mdq

all: build
build: go build

docker:
	docker build --no-cache=true -t $(NAME) .
	docker tag $(NAME) docker.sunet.se/$(NAME):$(VERSION)
push:
	docker push docker.sunet.se/$(NAME):$(VERSION)
