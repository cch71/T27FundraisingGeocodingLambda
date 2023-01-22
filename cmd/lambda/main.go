package main

import (
	"context"
	"log"

	"github.com/aws/aws-lambda-go/lambda"

	"github.com/cch71/T27FundraisingGeocoderLambda/frgeocoder"
)

////////////////////////////////////////////////////////////////////////////
//
func HandleLambdaEvent(ctx context.Context) error {
	err := frgeocoder.Init()
	if err != nil {
		log.Println("Failed to initialize db:", err)
		return err
	}
	defer frgeocoder.Deinit()

	flags := frgeocoder.UpdateGeoJsonFlags{
		UpdateDb: true,
	}
	err = frgeocoder.UpdateGeoJson(ctx, flags)
	if err != nil {
		log.Println("Failed to update GeoJSON:", err)
		return err
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////
//
func main() {
	lambda.Start(HandleLambdaEvent)
}
