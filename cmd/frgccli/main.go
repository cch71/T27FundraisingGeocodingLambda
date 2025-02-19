// usr/bin/env go run "$0" "$@"; exit
package main

import (
	"context"
	"log"

	"github.com/cch71/T27FundraisingGeocoderLambda/frgeocoder"

	"github.com/joho/godotenv"
)

var _ = godotenv.Load()

func main() {
	ctx := context.Background()
	if err := frgeocoder.Init(); err != nil {
		log.Panic("Failed to initialize db:", err)
	}
	defer frgeocoder.Deinit()

	flags := frgeocoder.UpdateGeoJsonFlags{
		UpdateDb:        true,
		DoCompleteRegen: false,
	}
	frgeocoder.UpdateGeoJson(ctx, flags)
}
