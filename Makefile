ifneq (,$(wildcard ./.env))
    include .env
    export
endif

MK_DIR := $(realpath $(dir $(lastword $(MAKEFILE_LIST))))
DIST_DIR := ${MK_DIR}/dist

clean:
		@rm -rf dist
		@mkdir -p dist

lambda: clean
		cd ${MK_DIR}/cmd/lambda && GOOS=linux CGO_ENABLED=0 GOARCH=arm64 go build -ldflags="-s -w" -o ${DIST_DIR}/bootstrap

cli: clean
		cd ${MK_DIR}/cmd/frgccli && go build -o ${DIST_DIR}/frgccli

dist: lambda
		cp $(DB_CA_ROOT_PATH) dist
		cd dist && zip function.zip bootstrap root.crt

deploy: dist
		op plugin run -- aws lambda update-function-code --function-name ${GEOCODER_LAMBDA_FUNCTION_NAME} --zip-file fileb://${PWD}/dist/function.zip

run:
		aws-sam-local local start-api

install:
		go get github.com/aws/aws-lambda-go/events
		go get github.com/aws/aws-lambda-go/lambda
		go get github.com/stretchr/testify/assert

install-dev:
		go get github.com/awslabs/aws-sam-local

tidy:
	cd frgeocoder && go mod tidy
	cd cmd/lambda && go mod tidy
	cd cmd/frgccli && go mod tidy

update-mods:
	cd frgeocoder && go get -u ./... && go mod tidy
	cd cmd/lambda && go get -u ./... && go mod tidy
	cd cmd/frgccli && go get -u ./... && go mod tidy

test:
		go test ./... --cover
