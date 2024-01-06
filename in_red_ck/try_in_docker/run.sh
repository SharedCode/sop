CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .
docker build -t try_in_docker .
docker run -p 6379:6379 --rm try_in_docker:latest
